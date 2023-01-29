#include "olap/rowset/segment_v2/inverted_index_writer.h"

#include <CLucene.h>
#include <CLucene/util/bkd/bkd_writer.h>

#include <memory>

#include "cloud/io/local_file_system.h"
#include "cloud/io/tmp_file_mgr.h"
#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_compound_directory.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/tablet_schema.h"
#include "util/string_util.h"

#define FINALIZE_OUTPUT(x) if (x != nullptr){x->close(); _CLDELETE(x);}
#define FINALLY_FINALIZE_OUTPUT(x) try{FINALIZE_OUTPUT(x)}catch(...){}

namespace doris::segment_v2 {
const int32_t MAX_FIELD_LEN = 0x7FFFFFFFL;
const int32_t MAX_LEAF_COUNT = 1024;
const float MAXMBSortInHeap = 512.0 * 8;
const int DIMS = 1;

template <FieldType field_type>
class InvertedIndexColumnWriterImpl : public InvertedIndexColumnWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;

public:
    explicit InvertedIndexColumnWriterImpl(const std::string& field_name, uint32_t uuid,
                                           const std::string& segment_file_name,
                                           const std::string& dir, io::FileSystemSPtr fs,
                                           const TabletIndex* index_meta)
            : _segment_file_name(segment_file_name),
              _directory(dir),
              _fs(std::move(fs)),
              _index_meta(index_meta) {
        _parser_type = get_inverted_index_parser_type_from_string(
                get_parser_string_from_properties(_index_meta->properties()));
        _value_key_coder = get_key_coder(field_type);
        _field_name = std::wstring(field_name.begin(), field_name.end());
    };

    ~InvertedIndexColumnWriterImpl() override {}

    Status init() override {
        try {
            if constexpr (field_is_slice_type(field_type)) {
                return init_fulltext_index();
            } else if constexpr (field_is_numeric_type(field_type)) {
                return init_bkd_index();
            }
            return Status::InternalError("field type not supported");
        } catch (const CLuceneError& e) {
            LOG(WARNING) << "Inverted index writer init error occurred: " << e.what();
            return Status::OLAPInternalError(
                    OLAP_ERR_INVERTED_INDEX_CLUCENE_ERROR,
                    fmt::format("Inverted index writer init error occurred, error msg: {}", e.what()));
        }
    }

    void close() {
        if (_index_writer) {
            _index_writer->close();
            if (config::enable_write_index_searcher_cache) {
                // open index searcher into cache
                auto index_file_name = InvertedIndexDescriptor::get_index_file_name(
                        _segment_file_name, _index_meta->index_id());
                InvertedIndexSearcherCache::instance()->insert(_fs, _directory, index_file_name);
            }
            _CLLDELETE(_index_writer);
            _index_writer = nullptr;
        }

        if (_doc) {
            _CLLDELETE(_doc);
            _doc = nullptr;
        }

        if (_default_analyzer) {
            _CLLDELETE(_default_analyzer);
            _default_analyzer = nullptr;
        }

        if (_default_char_analyzer) {
            _CLLDELETE(_default_char_analyzer);
            _default_char_analyzer = nullptr;
        }

        if (_standard_analyzer) {
            _CLLDELETE(_standard_analyzer);
            _standard_analyzer = nullptr;
        }
        if (_char_string_reader) {
            _CLDELETE(_char_string_reader);
            _char_string_reader = nullptr;
        }
    };

    Status init_bkd_index() {
        size_t value_length = sizeof(CppType);
        // NOTE: initialize with 0, set to max_row_id when finish.
        int32_t max_doc = 0;
        int32_t total_point_count = std::numeric_limits<std::int32_t>::max();
        _bkd_writer = std::make_shared<lucene::util::bkd::bkd_writer>(
                max_doc, DIMS, DIMS, value_length, MAX_LEAF_COUNT, MAXMBSortInHeap,
                total_point_count, true, config::max_depth_in_bkd_tree);
        return Status::OK();
    }

    Status init_fulltext_index() {
        bool create = true;

        auto index_path = InvertedIndexDescriptor::get_temporary_index_path(
                _directory + "/" + _segment_file_name, _index_meta->index_id());

        // LOG(INFO) << "inverted index path: " << index_path;

        if (lucene::index::IndexReader::indexExists(index_path.c_str())) {
            create = false;
            if (lucene::index::IndexReader::isLocked(index_path.c_str())) {
                LOG(INFO) << ("Lucene Index was locked... unlocking it.\n");
                lucene::index::IndexReader::unlock(index_path.c_str());
            }
        }

        _char_string_reader = _CLNEW lucene::util::SStringReader<char>;
        _doc = _CLNEW lucene::document::Document();
        _default_analyzer = _CLNEW lucene::analysis::SimpleAnalyzer<TCHAR>();
        _default_char_analyzer = _CLNEW lucene::analysis::SimpleAnalyzer<char>();
        _standard_analyzer = _CLNEW lucene::analysis::standard::StandardAnalyzer();
#ifdef CLOUD_MODE
        _lfs = std::make_shared<doris::io::LocalFileSystem>(
                io::TmpFileMgr::instance()->get_tmp_file_dir(), "");
        auto lfs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
                io::TmpFileMgr::instance()->get_tmp_file_dir() + "/" + _segment_file_name,
                _index_meta->index_id());
        _dir.reset(DorisCompoundDirectory::getDirectory(_lfs, lfs_index_path.c_str(),
                                                        true, _fs, index_path.c_str()));

#else
        _dir.reset(DorisCompoundDirectory::getDirectory(_fs, index_path.c_str(), true));
#endif

        if (_parser_type == InvertedIndexParserType::PARSER_STANDARD) {
            _index_writer =
                    _CLNEW lucene::index::IndexWriter(_dir.get(), _standard_analyzer, create, true);
        } else if (_parser_type == InvertedIndexParserType::PARSER_ENGLISH) {
            _index_writer = _CLNEW lucene::index::IndexWriter(_dir.get(), _default_char_analyzer,
                                                              create, true);
        } else {
            // ANALYSER_NOT_SET, ANALYSER_NONE use default SimpleAnalyzer
            _index_writer =
                    _CLNEW lucene::index::IndexWriter(_dir.get(), _default_analyzer, create, true);
        }

        _index_writer->setMaxBufferedDocs(config::inverted_index_max_buffer_docs);
        _index_writer->setRAMBufferSizeMB(config::inverted_index_ram_buffer_size);
        _index_writer->setMaxFieldLength(MAX_FIELD_LEN);
        _index_writer->setMergeFactor(config::inverted_index_merge_factor);
        _index_writer->setUseCompoundFile(false);
        _doc->clear();

        int field_config =
                lucene::document::Field::STORE_NO | lucene::document::Field::INDEX_NONORMS;
        if (_parser_type == InvertedIndexParserType::PARSER_NONE) {
            field_config |= lucene::document::Field::INDEX_UNTOKENIZED;
        } else {
            field_config |= lucene::document::Field::INDEX_TOKENIZED;
        }
        //auto name = lucene::util::Misc::_charToWide(fn.c_str());
        //field_name = std::wstring(fn.begin(), fn.end());

        _field = _CLNEW lucene::document::Field(_field_name.c_str(), field_config);
        _doc->add(*_field);
        // NOTE: need to ref_cnt-- for dir,
        // when index_writer is destroyed, if closeDir is set, dir will be close
        // _CLDECDELETE(dir) will try to ref_cnt--, when it decreases to 1, dir will be destroyed.
        //_CLLDECDELETE(dir)

        return Status::OK();
    }

    Status add_nulls(uint32_t count) override {
        _rid += count;
        if constexpr (field_is_slice_type(field_type)) {
            if (_field == nullptr) {
                LOG(ERROR) << "could not find field in fields map when add_nulls in inverted index "
                              "writer";
                return Status::InternalError("could not find field in clucene");
            }

            for (int i = 0; i < count; ++i) {
                std::string empty_value;
                auto empty_val = lucene::util::Misc::_charToWide(empty_value.c_str());
                _field->setValue(empty_val, false);
                _index_writer->addDocument(_doc);
            }
        }
        return Status::OK();
    }

    void new_fulltext_field(const char* field_value_data, size_t field_value_size) {
        if (_parser_type == InvertedIndexParserType::PARSER_ENGLISH) {
            new_char_token_stream(field_value_data, field_value_size, _field);
        } else if (_parser_type == InvertedIndexParserType::PARSER_STANDARD) {
            new_field_value(field_value_data, field_value_size, _field);
        } else {
            new_field_value(field_value_data, field_value_size, _field);
        }
    }

    void new_char_token_stream(const char* s, size_t len, lucene::document::Field* field) {
        _char_string_reader->init(s, len, false);
        auto stream =
                _default_char_analyzer->reusableTokenStream(field->name(), _char_string_reader);
        field->setValue(stream);
    }

    //NOTE:be careful that using this function will add document inside
    void new_string_reader(const char* s, size_t len, lucene::document::Field* field) {
        auto field_value = lucene::util::Misc::_charToWide(s, len);
        // NOTE: avoid another data copy for string reader init in documentWriterThreadState
        auto stringReader =
                _CLNEW lucene::util::StringReader(field_value, wcslen(field_value), false);
        field->setValue(stringReader);
        _index_writer->addDocument(_doc);
        _CLDELETE_ARRAY(field_value)
    }

    void new_field_value(const char* s, size_t len, lucene::document::Field* field) {
        auto field_value = lucene::util::Misc::_charToWide(s, len);
        field->setValue(field_value, false);
        // setValue did not duplicate value, so we don't have to delete
        //_CLDELETE_ARRAY(field_value)
    }

    Status add_values(const std::string fn, const void* values, size_t count) override {
        if constexpr (field_is_slice_type(field_type)) {
            if (_field == nullptr) {
                LOG(ERROR) << "could not find field in fields map when add_values in inverted "
                              "index writer"
                           << fn;
                return Status::InternalError("could not find field in clucene");
            }
            auto* v = (Slice*)values;
            for (int i = 0; i < count; ++i) {
                new_fulltext_field(v->get_data(), v->get_size());
                _index_writer->addDocument(_doc);
                ++v;
                _rid++;
            }
        } else if constexpr (field_is_numeric_type(field_type)) {
            add_numeric_values(values, count);
        }
        return Status::OK();
    }

    Status add_array_values(size_t field_size, const CollectionValue* values,
                            size_t count) override {
        auto* item_data_ptr = const_cast<CollectionValue*>(values)->mutable_data();

        if constexpr (field_is_slice_type(field_type)) {
            if (_field == nullptr) {
                LOG(ERROR)
                        << "could not find field in fields map when add_array_values in inverted "
                           "index writer";
                return Status::InternalError("could not find field in clucene");
            }
            for (int i = 0; i < count; ++i) {
                std::vector<std::string> strings;

                for (size_t j = 0; j < values->length(); ++j) {
                    auto* v = (Slice*)item_data_ptr;

                    if (!values->is_null_at(j)) {
                        strings.emplace_back(std::string(v->get_data(), v->get_size()));
                    }
                    item_data_ptr = (uint8_t*)item_data_ptr + field_size;
                }
                auto value = join(strings, " ");
                new_fulltext_field(value.c_str(), value.length());
                _rid++;
                _index_writer->addDocument(_doc);
            }
        } else if constexpr (field_is_numeric_type(field_type)) {
            auto p = reinterpret_cast<const CppType*>(item_data_ptr);
            for (int i = 0; i < count; ++i) {
                for (size_t j = 0; j < values->length(); ++j) {
                    if (values->is_null_at(j)) {
                        // bkd do not index null values, so we do nothing here.
                    } else {
                        std::string new_value;
                        size_t value_length = sizeof(CppType);

                        _value_key_coder->full_encode_ascending(p, &new_value);
                        _bkd_writer->add((const uint8_t*)new_value.c_str(), value_length, _rid);
                    }
                    p++;
                }
                _row_ids_seen_for_bkd++;
                _rid++;
            }
        }
        return Status::OK();
    }

    void add_numeric_values(const void* values, size_t count) {
        auto p = reinterpret_cast<const CppType*>(values);
        for (size_t i = 0; i < count; ++i) {
            add_value(*p);
            p++;
            _row_ids_seen_for_bkd++;
        }
    }

    void add_value(const CppType& value) {
        std::string new_value;
        size_t value_length = sizeof(CppType);

        _value_key_coder->full_encode_ascending(&value, &new_value);
        _bkd_writer->add((const uint8_t*)new_value.c_str(), value_length, _rid);

        _rid++;
    }

    uint64_t size() const override {
        //TODO: get size of inverted index
        return 0;
    }

    Status finish() override {
        lucene::store::Directory* dir = nullptr;
        lucene::store::IndexOutput* data_out= nullptr;
        lucene::store::IndexOutput* index_out= nullptr;
        lucene::store::IndexOutput* meta_out= nullptr;
        try {
            if constexpr (field_is_numeric_type(field_type)) {
                auto index_path = InvertedIndexDescriptor::get_temporary_index_path(
                        _directory + "/" + _segment_file_name, _index_meta->index_id());
#ifdef CLOUD_MODE
                if (_lfs == nullptr) {
                    _lfs = io::LocalFileSystem::create(io::TmpFileMgr::instance()->get_tmp_file_dir());
                }
                auto lfs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
                        io::TmpFileMgr::instance()->get_tmp_file_dir() + "/" + _segment_file_name,
                        _index_meta->index_id());
                dir = DorisCompoundDirectory::getDirectory(
                        _lfs, lfs_index_path.c_str(), true, _fs, index_path.c_str());
#else
                dir = DorisCompoundDirectory::getDirectory(_fs, index_path.c_str(), true);
#endif
                _bkd_writer->max_doc_ = _rid;
                _bkd_writer->docs_seen_ = _row_ids_seen_for_bkd;
		data_out = dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_bkd_index_data_file_name().c_str());
                meta_out = dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_bkd_index_meta_file_name().c_str());
                index_out = dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_bkd_index_file_name().c_str());
		if (data_out != nullptr && meta_out != nullptr && index_out != nullptr){
                    _bkd_writer->meta_finish(meta_out,
                                             _bkd_writer->finish(data_out, index_out),
                                             field_type);
                }
                FINALIZE_OUTPUT(meta_out)
                FINALIZE_OUTPUT(data_out)
                FINALIZE_OUTPUT(index_out)
                FINALIZE_OUTPUT(dir)
            } else if constexpr (field_is_slice_type(field_type)) {
                close();
            }
        } catch (CLuceneError& e) {
	    LOG(WARNING) << "InvertedIndexColumnWriter finish catch exception: " << e.what()
                         << ", meta_out: " << meta_out << ", data_out: " << data_out
                         << ", index_out: " << index_out;
            FINALLY_FINALIZE_OUTPUT(meta_out)
            FINALLY_FINALIZE_OUTPUT(data_out)
            FINALLY_FINALIZE_OUTPUT(index_out)
            FINALLY_FINALIZE_OUTPUT(dir)
            LOG(WARNING) << "Inverted index writer finish error occurred: " << e.what();
            return Status::OLAPInternalError(
                    OLAP_ERR_INVERTED_INDEX_CLUCENE_ERROR,
                    fmt::format("Inverted index writer finish error occurred, error msg: {}", e.what()));
        }

        return Status::OK();
    }

private:
    //std::shared_ptr<MemTracker> _tracker;
    //MemPool _pool;
    //const TypeInfo* _typeinfo;
    rowid_t _rid = 0;
    uint32_t _row_ids_seen_for_bkd = 0;
    roaring::Roaring _null_bitmap;
    uint64_t _reverted_index_size;

    lucene::document::Document* _doc{};
    lucene::document::Field* _field{};
    lucene::index::IndexWriter* _index_writer{};
    lucene::util::SStringReader<char>* _char_string_reader{};
    lucene::analysis::SimpleAnalyzer<TCHAR>* _default_analyzer{};
    lucene::analysis::SimpleAnalyzer<char>* _default_char_analyzer{};
    lucene::analysis::standard::StandardAnalyzer* _standard_analyzer{};
    std::shared_ptr<lucene::util::bkd::bkd_writer> _bkd_writer;
    std::string _segment_file_name;
    std::string _directory;
    io::FileSystemSPtr _fs;
#ifdef CLOUD_MODE
    io::FileSystemSPtr _lfs;
#endif
    const KeyCoder* _value_key_coder;
    const TabletIndex* _index_meta;
    InvertedIndexParserType _parser_type;
    std::wstring _field_name;
    std::unique_ptr<DorisCompoundDirectory> _dir;
};

Status InvertedIndexColumnWriter::create(const Field* field,
                                         std::unique_ptr<InvertedIndexColumnWriter>* res,
                                         uint32_t uuid, const std::string& segment_file_name,
                                         const std::string& dir, const TabletIndex* index_meta,
                                         io::FileSystemSPtr fs) {
    //RETURN_IF_ERROR(InvertedIndexDescriptor::init_index_directory(path));

    auto typeinfo = field->type_info();
    FieldType type = typeinfo->type();
    std::string field_name = field->name();
    if (type == OLAP_FIELD_TYPE_ARRAY) {
        const auto array_typeinfo = dynamic_cast<const ArrayTypeInfo*>(typeinfo);
        typeinfo = array_typeinfo->item_type_info();
        type = typeinfo->type();
    }

    switch (type) {
    case OLAP_FIELD_TYPE_CHAR: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_CHAR>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_VARCHAR: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_VARCHAR>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_STRING: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_STRING>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_DATETIME: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_DATETIME>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_DATE: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_DATE>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_TINYINT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_TINYINT>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_SMALLINT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_SMALLINT>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_UNSIGNED_INT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_UNSIGNED_INT>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_INT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_INT>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_LARGEINT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_LARGEINT>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_DECIMAL: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_DECIMAL>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_BOOL: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_BOOL>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_DOUBLE: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_DOUBLE>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_FLOAT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_FLOAT>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_BIGINT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_BIGINT>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        break;
    }
    default:
        return Status::NotSupported("unsupported type for inverted index: " + std::to_string(type));
    }
    if (*res != nullptr) {
        RETURN_IF_ERROR((*res)->init());
    }
    return Status::OK();
}
} // namespace doris::segment_v2

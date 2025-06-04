// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "gtest/gtest.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/hierarchical_data_reader.h"
#include "olap/rowset/segment_v2/variant_column_writer_impl.h"
#include "olap/storage_engine.h"
#include "testutil/schema_utils.h"
#include "testutil/variant_util.h"

using namespace doris::vectorized;

namespace doris {

constexpr static uint32_t MAX_PATH_LEN = 1024;
constexpr static std::string_view dest_dir = "/ut_dir/variant_column_writer_test";
constexpr static std::string_view tmp_dir = "./ut_dir/tmp";

class VariantColumnWriterReaderTest : public testing::Test {
public:
    void SetUp() override {
        // absolute dir
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        _current_dir = std::string(buffer);
        _absolute_dir = _current_dir + std::string(dest_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());

        // tmp dir
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tmp_dir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(std::string(tmp_dir), 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        Status st = tmp_file_dirs->init();
        EXPECT_TRUE(st.ok()) << st.to_json();
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        // storage engine
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine_ref = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
        static_cast<void>(_data_dir->update_capacity());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        _engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    VariantColumnWriterReaderTest() = default;
    ~VariantColumnWriterReaderTest() override = default;

private:
    TabletSchemaSPtr _tablet_schema = nullptr;
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir = nullptr;
    TabletSharedPtr _tablet = nullptr;
    std::string _absolute_dir;
    std::string _current_dir;
};

void check_column_meta(const ColumnMetaPB& column_meta, auto& path_with_size) {
    EXPECT_TRUE(column_meta.has_column_path_info());
    auto path = std::make_shared<vectorized::PathInData>();
    path->from_protobuf(column_meta.column_path_info());
    EXPECT_EQ(column_meta.column_path_info().parrent_column_unique_id(), 1);
    EXPECT_EQ(column_meta.none_null_size(), path_with_size[path->copy_pop_front().get_path()]);
}

void check_sparse_column_meta(const ColumnMetaPB& column_meta, auto& path_with_size) {
    EXPECT_TRUE(column_meta.has_column_path_info());
    auto path = std::make_shared<vectorized::PathInData>();
    path->from_protobuf(column_meta.column_path_info());
    EXPECT_EQ(column_meta.column_path_info().parrent_column_unique_id(), 1);
    for (const auto& [path, size] :
         column_meta.variant_statistics().sparse_column_non_null_size()) {
        EXPECT_EQ(size, path_with_size[path]);
    }
    EXPECT_EQ(path->copy_pop_front().get_path(), "__DORIS_VARIANT_SPARSE__");
}

TEST_F(VariantColumnWriterReaderTest, test_statics) {
    VariantStatisticsPB stats_pb;
    auto* subcolumns_stats = stats_pb.mutable_sparse_column_non_null_size();
    (*subcolumns_stats)["key0"] = 500;  // 50% of rows have key0
    (*subcolumns_stats)["key1"] = 500;  // 50% of rows have key1
    (*subcolumns_stats)["key2"] = 333;  // 33.3% of rows have key2
    (*subcolumns_stats)["key3"] = 200;  // 20% of rows have key3
    (*subcolumns_stats)["key4"] = 1000; // 100% of rows have key4

    auto* sparse_stats = stats_pb.mutable_sparse_column_non_null_size();
    (*sparse_stats)["key5"] = 100;
    (*sparse_stats)["key6"] = 200;
    (*sparse_stats)["key7"] = 300;

    // 6.2 Test from_pb
    segment_v2::VariantStatistics stats;
    stats.from_pb(stats_pb);

    // 6.3 Verify statistics
    EXPECT_EQ(stats.sparse_column_non_null_size["key0"], 500);
    EXPECT_EQ(stats.sparse_column_non_null_size["key1"], 500);
    EXPECT_EQ(stats.sparse_column_non_null_size["key2"], 333);
    EXPECT_EQ(stats.sparse_column_non_null_size["key3"], 200);
    EXPECT_EQ(stats.sparse_column_non_null_size["key4"], 1000);

    EXPECT_EQ(stats.sparse_column_non_null_size["key5"], 100);
    EXPECT_EQ(stats.sparse_column_non_null_size["key6"], 200);
    EXPECT_EQ(stats.sparse_column_non_null_size["key7"], 300);
}

TEST_F(VariantColumnWriterReaderTest, test_write_data_normal) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    SchemaUtils::construct_column(schema_pb.add_column(), 1, "VARIANT", "V1");
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());

    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto path_with_size =
            VariantUtil::fill_object_column_with_test_data(column_object, 1000, &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 5);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    for (int i = 1; i < footer.columns_size() - 1; ++i) {
        auto column_meta = footer.columns(i);
        check_column_meta(column_meta, path_with_size);
    }
    check_sparse_column_meta(footer.columns(footer.columns_size() - 1), path_with_size);

    // 7. check variant reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    ColumnReaderOptions read_opts;
    read_opts.tablet_schema = _tablet_schema;
    std::unique_ptr<ColumnReader> column_reader;
    st = ColumnReader::create(read_opts, footer, 0, 1000, file_reader, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    auto subcolumn_reader = variant_column_reader->get_reader_by_path(PathInData("key0"));
    EXPECT_TRUE(subcolumn_reader != nullptr);
    subcolumn_reader = variant_column_reader->get_reader_by_path(PathInData("key1"));
    EXPECT_TRUE(subcolumn_reader != nullptr);
    subcolumn_reader = variant_column_reader->get_reader_by_path(PathInData("key2"));
    EXPECT_TRUE(subcolumn_reader != nullptr);
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key3")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key4")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key5")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key6")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key7")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key8")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key9")));
    auto size = variant_column_reader->get_metadata_size();
    EXPECT_GT(size, 0);

    // 8. check statistics
    auto statistics = variant_column_reader->get_stats();
    for (const auto& [path, size] : statistics->subcolumns_non_null_size) {
        EXPECT_EQ(path_with_size[path], size);
    }
    for (const auto& [path, size] : statistics->sparse_column_non_null_size) {
        EXPECT_EQ(path_with_size[path], size);
    }

    // 9. check hier reader
    ColumnIterator* it;
    TabletColumn parent_column = _tablet_schema->column(0);
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    st = variant_column_reader->new_iterator(&it, &parent_column, &storage_read_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<HierarchicalDataReader*>(it) != nullptr);
    ColumnIteratorOptions column_iter_opts;
    OlapReaderStatistics stats;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    MutableColumnPtr new_column_object = ColumnObject::create(3);
    size_t nrows = 1000;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, new_column_object);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);

    for (int i = 0; i < 1000; ++i) {
        std::string value;
        assert_cast<ColumnObject*>(new_column_object.get())->serialize_one_row_to_string(i, &value);

        EXPECT_EQ(value, inserted_jsonstr[i]);
    }

    std::vector<rowid_t> row_ids;
    for (int i = 0; i < 1000; ++i) {
        if (i % 7 == 0) {
            row_ids.push_back(i);
        }
    }
    new_column_object = ColumnObject::create(3);
    st = it->read_by_rowids(row_ids.data(), row_ids.size(), new_column_object);
    EXPECT_TRUE(st.ok()) << st.msg();
    for (int i = 0; i < row_ids.size(); ++i) {
        std::string value;
        assert_cast<ColumnObject*>(new_column_object.get())->serialize_one_row_to_string(i, &value);
        EXPECT_EQ(value, inserted_jsonstr[row_ids[i]]);
    }

    auto read_to_column_object = [&](ColumnIterator* it) {
        new_column_object = ColumnObject::create(3);
        nrows = 1000;
        st = it->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = it->next_batch(&nrows, new_column_object);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(stats.bytes_read > 0);
        EXPECT_EQ(nrows, 1000);
    };
    delete (it);

    // 10. check sparse extract reader
    for (int i = 3; i < 10; ++i) {
        std::string key = ".key" + std::to_string(i);
        TabletColumn subcolumn_in_sparse;
        subcolumn_in_sparse.set_name(parent_column.name_lower_case() + key);
        subcolumn_in_sparse.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
        subcolumn_in_sparse.set_parent_unique_id(parent_column.unique_id());
        subcolumn_in_sparse.set_path_info(PathInData(parent_column.name_lower_case() + key));
        subcolumn_in_sparse.set_variant_max_subcolumns_count(
                parent_column.variant_max_subcolumns_count());
        subcolumn_in_sparse.set_is_nullable(true);

        ColumnIterator* it;
        st = variant_column_reader->new_iterator(&it, &subcolumn_in_sparse, &storage_read_opts);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(assert_cast<SparseColumnExtractReader*>(it) != nullptr);
        st = it->init(column_iter_opts);
        EXPECT_TRUE(st.ok()) << st.msg();

        read_to_column_object(it);

        for (int row = 0; row < 1000; ++row) {
            std::string value;
            assert_cast<ColumnObject*>(new_column_object.get())
                    ->serialize_one_row_to_string(row, &value);
            if (inserted_jsonstr[row].find(key) != std::string::npos) {
                if (i % 2 == 0) {
                    EXPECT_EQ(value, "88");
                } else {
                    EXPECT_EQ(value, "str99");
                }
            }
        }
        delete (it);
    }

    // 11. check leaf reader
    auto check_leaf_reader = [&]() {
        for (int i = 0; i < 3; ++i) {
            std::string key = ".key" + std::to_string(i);
            TabletColumn subcolumn;
            subcolumn.set_name(parent_column.name_lower_case() + key);
            subcolumn.set_type((FieldType)(int)footer.columns(i + 1).type());
            subcolumn.set_parent_unique_id(parent_column.unique_id());
            subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + key));
            subcolumn.set_variant_max_subcolumns_count(
                    parent_column.variant_max_subcolumns_count());
            subcolumn.set_is_nullable(true);

            ColumnIterator* it;
            st = variant_column_reader->new_iterator(&it, &subcolumn, &storage_read_opts);
            EXPECT_TRUE(st.ok()) << st.msg();
            EXPECT_TRUE(assert_cast<FileColumnIterator*>(it) != nullptr);
            st = it->init(column_iter_opts);
            EXPECT_TRUE(st.ok()) << st.msg();

            auto column_type = DataTypeFactory::instance().create_data_type(subcolumn, false);
            auto read_column = column_type->create_column();
            nrows = 1000;
            st = it->seek_to_ordinal(0);
            EXPECT_TRUE(st.ok()) << st.msg();
            st = it->next_batch(&nrows, read_column);
            EXPECT_TRUE(st.ok()) << st.msg();
            EXPECT_TRUE(stats.bytes_read > 0);

            for (int row = 0; row < 1000; ++row) {
                const std::string& value = column_type->to_string(*read_column, row);
                if (inserted_jsonstr[row].find(key) != std::string::npos) {
                    if (i % 2 == 0) {
                        EXPECT_EQ(value, "88");
                    } else {
                        EXPECT_EQ(value, "str99");
                    }
                }
            }
            delete (it);
        }
    };
    check_leaf_reader();

    // 12. check empty
    TabletColumn subcolumn;
    subcolumn.set_name(parent_column.name_lower_case() + ".key10");
    subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    subcolumn.set_parent_unique_id(parent_column.unique_id());
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".key10"));
    subcolumn.set_is_nullable(true);
    ColumnIterator* it1;
    st = variant_column_reader->new_iterator(&it1, &subcolumn, &storage_read_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<DefaultValueColumnIterator*>(it1) != nullptr);

    // 13. check statistics size == limit
    auto& variant_stats = variant_column_reader->_statistics;
    EXPECT_TRUE(variant_stats->sparse_column_non_null_size.size() <
                config::variant_max_sparse_column_statistics_size);
    auto limit = config::variant_max_sparse_column_statistics_size -
                 variant_stats->sparse_column_non_null_size.size();
    for (int i = 0; i < limit; ++i) {
        std::string key = parent_column.name_lower_case() + ".key10" + std::to_string(i);
        variant_stats->sparse_column_non_null_size[key] = 10000;
    }
    EXPECT_TRUE(variant_stats->sparse_column_non_null_size.size() ==
                config::variant_max_sparse_column_statistics_size);
    delete (it1);

    ColumnIterator* it2;
    st = variant_column_reader->new_iterator(&it2, &subcolumn, &storage_read_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<HierarchicalDataReader*>(it2) != nullptr);
    st = it2->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto check_empty_column = [&]() {
        for (int row = 0; row < 1000; ++row) {
            std::string value;
            assert_cast<ColumnObject*>(new_column_object.get())
                    ->serialize_one_row_to_string(row, &value);

            EXPECT_EQ(value, "{}");
        }
    };

    read_to_column_object(it2);
    check_empty_column();

    // construct tablet schema for compaction
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
    storage_read_opts.tablet_schema = _tablet_schema;
    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> uid_to_paths_set_info;
    TabletSchema::PathsSetInfo paths_set_info;
    paths_set_info.sub_path_set.insert("key0");
    paths_set_info.sub_path_set.insert("key3");
    paths_set_info.sub_path_set.insert("key4");
    paths_set_info.sparse_path_set.insert("key1");
    paths_set_info.sparse_path_set.insert("key2");
    paths_set_info.sparse_path_set.insert("key5");
    paths_set_info.sparse_path_set.insert("key6");
    paths_set_info.sparse_path_set.insert("key7");
    paths_set_info.sparse_path_set.insert("key8");
    paths_set_info.sparse_path_set.insert("key9");
    uid_to_paths_set_info[parent_column.unique_id()] = paths_set_info;
    _tablet_schema->set_path_set_info(std::move(uid_to_paths_set_info));

    // 14. check compaction subcolumn reader
    check_leaf_reader();
    delete (it2);
    // 15. check compaction root reader
    ColumnIterator* it3;
    st = variant_column_reader->new_iterator(&it3, &parent_column, &storage_read_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<VariantRootColumnIterator*>(it3) != nullptr);
    st = it3->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    delete (it3);
    // 16. check compacton sparse column
    TabletColumn sparse_column = schema_util::create_sparse_column(parent_column);
    ColumnIterator* it4;
    st = variant_column_reader->new_iterator(&it4, &sparse_column, &storage_read_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<SparseColumnMergeReader*>(it4) != nullptr);
    st = it4->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto column_type = DataTypeFactory::instance().create_data_type(sparse_column, false);
    auto read_column = column_type->create_column();
    nrows = 1000;
    st = it4->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it4->next_batch(&nrows, read_column);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);

    for (int row = 0; row < 1000; ++row) {
        const std::string& value = column_type->to_string(*read_column, row);
        EXPECT_TRUE(value.find("key0") == std::string::npos)
                << "row: " << row << ", value: " << value;
        EXPECT_TRUE(value.find("key3") == std::string::npos)
                << "row: " << row << ", value: " << value;
        EXPECT_TRUE(value.find("key4") == std::string::npos)
                << "row: " << row << ", value: " << value;
    }

    delete (it4);
    // 17. check limit = 10000
    subcolumn.set_name(parent_column.name_lower_case() + ".key10");
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".key10"));
    ColumnIterator* it5;
    st = variant_column_reader->new_iterator(&it5, &subcolumn, &storage_read_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<SparseColumnExtractReader*>(it5) != nullptr);

    for (int i = 0; i < limit; ++i) {
        std::string key = parent_column.name_lower_case() + ".key10" + std::to_string(i);
        variant_stats->sparse_column_non_null_size.erase(key);
    }
    delete (it5);

    // 18. check compacton sparse extract column
    ColumnIterator* it6;
    subcolumn.set_name(parent_column.name_lower_case() + ".key3");
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".key3"));
    st = variant_column_reader->new_iterator(&it6, &subcolumn, &storage_read_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<SparseColumnExtractReader*>(it6) != nullptr);
    delete (it6);

    // 19. check compaction default column
    subcolumn.set_name(parent_column.name_lower_case() + ".key10");
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".key10"));
    ColumnIterator* it7;
    st = variant_column_reader->new_iterator(&it7, &subcolumn, &storage_read_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<DefaultValueColumnIterator*>(it7) != nullptr);
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    delete (it7);
}

TEST_F(VariantColumnWriterReaderTest, test_write_data_advanced) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    SchemaUtils::construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto path_with_size = VariantUtil::fill_object_column_with_nested_test_data(column_object, 1000,
                                                                                &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    for (int i = 1; i < footer.columns_size() - 1; ++i) {
        auto column_meta = footer.columns(i);
        check_column_meta(column_meta, path_with_size);
    }
    check_sparse_column_meta(footer.columns(footer.columns_size() - 1), path_with_size);

    // 7. check variant reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    ColumnReaderOptions read_opts;
    read_opts.tablet_schema = _tablet_schema;
    std::unique_ptr<ColumnReader> column_reader;
    st = ColumnReader::create(read_opts, footer, 0, 1000, file_reader, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    // 8. check statistics
    auto statistics = variant_column_reader->get_stats();
    for (const auto& [path, size] : statistics->subcolumns_non_null_size) {
        EXPECT_EQ(path_with_size[path], size);
    }
    for (const auto& [path, size] : statistics->sparse_column_non_null_size) {
        EXPECT_EQ(path_with_size[path], size);
    }

    // 9. check root
    ColumnIterator* it;
    TabletColumn parent_column = _tablet_schema->column(0);
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    st = variant_column_reader->new_iterator(&it, &parent_column, &storage_read_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<HierarchicalDataReader*>(it) != nullptr);
    ColumnIteratorOptions column_iter_opts;
    OlapReaderStatistics stats;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    MutableColumnPtr new_column_object = ColumnObject::create(3);
    size_t nrows = 1000;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, new_column_object);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);

    for (int i = 0; i < 1000; ++i) {
        std::string value;
        assert_cast<ColumnObject*>(new_column_object.get())->serialize_one_row_to_string(i, &value);
        EXPECT_EQ(value, inserted_jsonstr[i]);
    }

    auto read_to_column_object = [&](ColumnIterator* it) {
        new_column_object = ColumnObject::create(10);
        nrows = 1000;
        st = it->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = it->next_batch(&nrows, new_column_object);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(stats.bytes_read > 0);
        EXPECT_EQ(nrows, 1000);
    };
    delete (it);

    auto check_key_stats = [&](const std::string& key_num) {
        std::string key = ".key" + key_num;
        TabletColumn subcolumn_in_nested;
        subcolumn_in_nested.set_name(parent_column.name_lower_case() + key);
        subcolumn_in_nested.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
        subcolumn_in_nested.set_parent_unique_id(parent_column.unique_id());
        subcolumn_in_nested.set_path_info(PathInData(parent_column.name_lower_case() + key));
        subcolumn_in_nested.set_variant_max_subcolumns_count(
                parent_column.variant_max_subcolumns_count());
        subcolumn_in_nested.set_is_nullable(true);

        ColumnIterator* it1;
        st = variant_column_reader->new_iterator(&it1, &subcolumn_in_nested, &storage_read_opts);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(assert_cast<HierarchicalDataReader*>(it1) != nullptr);
        st = it1->init(column_iter_opts);
        EXPECT_TRUE(st.ok()) << st.msg();
        read_to_column_object(it1);

        size_t key_count = 0;
        size_t key_nested_count = 0;
        for (int row = 0; row < 1000; ++row) {
            std::string value;
            assert_cast<ColumnObject*>(new_column_object.get())
                    ->serialize_one_row_to_string(row, &value);
            if (value.find("nested" + key_num) != std::string::npos) {
                key_nested_count++;
            } else if (value.find("88") != std::string::npos) {
                key_count++;
            }
        }
        EXPECT_EQ(key_count, path_with_size["key" + key_num]);
        EXPECT_EQ(key_nested_count, path_with_size["key" + key_num + ".nested" + key_num]);
        delete (it1);
    };

    for (int i = 3; i < 10; ++i) {
        check_key_stats(std::to_string(i));
    }

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_write_sub_index) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
    SchemaUtils::construct_column(schema_pb.add_column(), 1, "VARIANT", "v", 2, false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);
    TabletColumn& variant = _tablet_schema->mutable_column_by_uid(1);
    // add subcolumn
    TabletColumn subcolumn2;
    subcolumn2.set_name("v.b");
    subcolumn2.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    variant.add_sub_column(subcolumn2);
    variant.set_is_bf_column(true);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto column_object = VariantUtil::construct_basic_varint_column();
    auto vw = assert_cast<VariantColumnWriter*>(writer.get());

    std::unique_ptr<VariantColumnData> _variant_column_data = std::make_unique<VariantColumnData>();
    _variant_column_data->column_data = column_object;
    _variant_column_data->row_pos = 0;
    const uint8_t* data = (const uint8_t*)_variant_column_data.get();
    st = vw->append_data(&data, 10);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_bloom_filter_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_bitmap_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(10);

    // 6. check footer
    std::cout << footer.columns_size() << std::endl;
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_write_data_nullable) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    SchemaUtils::construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    auto nullable_object = assert_cast<ColumnNullable*>(
            (*std::move(block.get_by_position(0).column)).mutate().get());
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto column_object = nullable_object->get_nested_column_ptr();
    schema_util::PathToNoneNullValues path_with_size;
    for (int idx = 0; idx < 10; idx++) {
        nullable_object->insert_default(); // insert null
        auto res = VariantUtil::fill_object_column_with_test_data(column_object, 80,
                                                                  &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        nullable_object->get_null_map_column_ptr()->insert_many(UInt8(0), 80);
        nullable_object->insert_many_defaults(17);
        res = VariantUtil::fill_object_column_with_test_data(column_object, 2, &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        nullable_object->get_null_map_column_ptr()->insert_many(UInt8(0), 2);
    }
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    auto size = vw->estimate_buffer_size();
    std::cout << "size: " << size << std::endl;
    st = vw->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    for (int i = 1; i < footer.columns_size() - 1; ++i) {
        auto column_meta = footer.columns(i);
        EXPECT_TRUE(column_meta.has_column_path_info());
        auto path = std::make_shared<vectorized::PathInData>();
        EXPECT_EQ(column_meta.column_path_info().parrent_column_unique_id(), 1);
        EXPECT_GT(column_meta.none_null_size(), path_with_size[path->copy_pop_front().get_path()]);
    }
    check_sparse_column_meta(footer.columns(footer.columns_size() - 1), path_with_size);

    // 7. check variant reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    ColumnReaderOptions read_opts;
    read_opts.tablet_schema = _tablet_schema;
    std::unique_ptr<ColumnReader> column_reader;
    st = ColumnReader::create(read_opts, footer, 0, 1000, file_reader, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    // 8. check statistics
    auto statistics = variant_column_reader->get_stats();
    for (const auto& [path, size] : statistics->subcolumns_non_null_size) {
        EXPECT_GT(size, path_with_size[path]);
    }
    for (const auto& [path, size] : statistics->sparse_column_non_null_size) {
        EXPECT_EQ(path_with_size[path], size);
    }

    // 9. check root
    ColumnIterator* it;
    TabletColumn parent_column = _tablet_schema->column(0);
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    st = variant_column_reader->new_iterator(&it, &parent_column, &storage_read_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<HierarchicalDataReader*>(it) != nullptr);
    ColumnIteratorOptions column_iter_opts;
    OlapReaderStatistics stats;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    delete (it);

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_write_data_nullable_without_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    SchemaUtils::construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    auto nullable_object = assert_cast<ColumnNullable*>(
            (*std::move(block.get_by_position(0).column)).mutate().get());
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto column_object = nullable_object->get_nested_column_ptr();
    schema_util::PathToNoneNullValues path_with_size;
    for (int idx = 0; idx < 10; idx++) {
        nullable_object->insert_default(); // insert null
        auto res = VariantUtil::fill_object_column_with_test_data(column_object, 80,
                                                                  &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        nullable_object->get_null_map_column_ptr()->insert_many(UInt8(0), 80);
        nullable_object->insert_many_defaults(17);
        res = VariantUtil::fill_object_column_with_test_data(column_object, 2, &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        nullable_object->get_null_map_column_ptr()->insert_many(UInt8(0), 2);
    }
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_write_bm_without_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    SchemaUtils::construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    auto nullable_object = assert_cast<ColumnNullable*>(
            (*std::move(block.get_by_position(0).column)).mutate().get());
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto column_object = nullable_object->get_nested_column_ptr();
    schema_util::PathToNoneNullValues path_with_size;
    for (int idx = 0; idx < 10; idx++) {
        nullable_object->insert_default(); // insert null
        auto res = VariantUtil::fill_object_column_with_test_data(column_object, 80,
                                                                  &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        nullable_object->get_null_map_column_ptr()->insert_many(UInt8(0), 80);
        nullable_object->insert_many_defaults(17);
        res = VariantUtil::fill_object_column_with_test_data(column_object, 2, &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        nullable_object->get_null_map_column_ptr()->insert_many(UInt8(0), 2);
    }
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_bitmap_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_write_bf_without_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    SchemaUtils::construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    auto nullable_object = assert_cast<ColumnNullable*>(
            (*std::move(block.get_by_position(0).column)).mutate().get());
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto column_object = nullable_object->get_nested_column_ptr();
    schema_util::PathToNoneNullValues path_with_size;
    for (int idx = 0; idx < 10; idx++) {
        nullable_object->insert_default(); // insert null
        auto res = VariantUtil::fill_object_column_with_test_data(column_object, 80,
                                                                  &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        nullable_object->get_null_map_column_ptr()->insert_many(UInt8(0), 80);
        nullable_object->insert_many_defaults(17);
        res = VariantUtil::fill_object_column_with_test_data(column_object, 2, &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        nullable_object->get_null_map_column_ptr()->insert_many(UInt8(0), 2);
    }
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_bloom_filter_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_write_zm_without_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    SchemaUtils::construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    auto nullable_object = assert_cast<ColumnNullable*>(
            (*std::move(block.get_by_position(0).column)).mutate().get());
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto column_object = nullable_object->get_nested_column_ptr();
    schema_util::PathToNoneNullValues path_with_size;
    for (int idx = 0; idx < 10; idx++) {
        nullable_object->insert_default(); // insert null
        auto res = VariantUtil::fill_object_column_with_test_data(column_object, 80,
                                                                  &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        nullable_object->get_null_map_column_ptr()->insert_many(UInt8(0), 80);
        nullable_object->insert_many_defaults(17);
        res = VariantUtil::fill_object_column_with_test_data(column_object, 2, &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        nullable_object->get_null_map_column_ptr()->insert_many(UInt8(0), 2);
    }
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_write_inverted_without_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    SchemaUtils::construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    auto nullable_object = assert_cast<ColumnNullable*>(
            (*std::move(block.get_by_position(0).column)).mutate().get());
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto column_object = nullable_object->get_nested_column_ptr();
    schema_util::PathToNoneNullValues path_with_size;
    for (int idx = 0; idx < 10; idx++) {
        nullable_object->insert_default(); // insert null
        auto res = VariantUtil::fill_object_column_with_test_data(column_object, 80,
                                                                  &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        nullable_object->get_null_map_column_ptr()->insert_many(UInt8(0), 80);
        nullable_object->insert_many_defaults(17);
        res = VariantUtil::fill_object_column_with_test_data(column_object, 2, &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        nullable_object->get_null_map_column_ptr()->insert_many(UInt8(0), 2);
    }
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_inverted_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

} // namespace doris

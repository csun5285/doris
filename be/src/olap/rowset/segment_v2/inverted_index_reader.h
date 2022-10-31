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

#pragma once

#include <CLucene.h>
#include <CLucene/util/BitSet.h>
#include <CLucene/util/bkd/bkd_reader.h>

#include <roaring/roaring.hh>

#include "common/status.h"
#include "env/env.h"
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/macros.h"
#include "io/fs/file_system.h"
#include "olap/olap_common.h"
#include "olap/inverted_index_parser.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"

namespace doris {
class KeyCoder;
class TypeInfo;

namespace segment_v2 {

class InvertedIndexIterator;

enum class InvertedIndexReaderType {
    UNKNOWN = -1,
    FULLTEXT = 0,
    STRING_TYPE = 1,
    BKD = 2,
};

enum class InvertedIndexQueryType {
    UNKNOWN_QUERY = -1,
    EQUAL_QUERY = 0,
    LESS_THAN_QUERY = 1,
    LESS_EQUAL_QUERY = 2,
    GREATER_THAN_QUERY = 3,
    GREATER_EQUAL_QUERY = 4,
    MATCH_ANY_QUERY = 5,
    MATCH_ALL_QUERY = 6,
    MATCH_PHRASE_QUERY = 7,
};

class InvertedIndexReader {
public:
    explicit InvertedIndexReader(io::FileSystem* fs, const std::string& path,
                                 const uint32_t uniq_id)
            : _fs(fs), _path(path), _uuid(uniq_id) {};
    virtual ~InvertedIndexReader() = default;

    // create a new column iterator. Client should delete returned iterator
    virtual Status new_iterator(InvertedIndexParserType analyser_type,
                                InvertedIndexIterator** iterator) = 0;
    virtual Status query(const std::string& column_name, const void* query_value,
                         InvertedIndexQueryType query_type, InvertedIndexParserType analyser_type,
                         roaring::Roaring* bit_map) = 0;
    virtual Status try_query(const std::string& column_name, const void* query_value,
                         InvertedIndexQueryType query_type, InvertedIndexParserType analyser_type,
                         uint32_t* count) = 0;

    virtual InvertedIndexReaderType type() = 0; 
    bool indexExists(io::Path& index_file_path);

protected:
    bool _is_match_query(InvertedIndexQueryType query_type);
    friend class InvertedIndexIterator;
    io::FileSystem* _fs;
    std::string _path;
    uint32_t _uuid;
};

class FullTextIndexReader : public InvertedIndexReader {
public:
    explicit FullTextIndexReader(io::FileSystem* fs, const std::string& path,
                                 const uint32_t uniq_id)
            : InvertedIndexReader(fs, path, uniq_id) {};
    ~FullTextIndexReader() override = default;

    Status new_iterator(InvertedIndexParserType analyser_type,
                        InvertedIndexIterator** iterator) override;
    Status query(const std::string& column_name, const void* query_value,
                 InvertedIndexQueryType query_type, InvertedIndexParserType analyser_type,
                 roaring::Roaring* bit_map) override;
    Status try_query(const std::string& column_name, const void* query_value,
                     InvertedIndexQueryType query_type, InvertedIndexParserType analyser_type,
                     uint32_t* count) override { return Status::OLAPInternalError(OLAP_ERR_FUNC_NOT_IMPLEMENTED); }

    InvertedIndexReaderType type() override;
    std::vector<std::string> get_analyse_result(const std::wstring& field_name,
                                                const std::wstring& value,
                                                InvertedIndexQueryType query_type,
                                                InvertedIndexParserType analyser_type);
};

class StringTypeInvertedIndexReader : public InvertedIndexReader {
public:
    explicit StringTypeInvertedIndexReader(io::FileSystem* fs, const std::string& path,
                                           const uint32_t uniq_id)
            : InvertedIndexReader(fs, path, uniq_id) {};
    ~StringTypeInvertedIndexReader() override = default;

    Status new_iterator(InvertedIndexParserType analyser_type,
                        InvertedIndexIterator** iterator) override;
    Status query(const std::string& column_name, const void* query_value,
                 InvertedIndexQueryType query_type, InvertedIndexParserType analyser_type,
                 roaring::Roaring* bit_map) override;
    Status try_query(const std::string& column_name, const void* query_value,
                 InvertedIndexQueryType query_type, InvertedIndexParserType analyser_type,
                 uint32_t* count) override { return Status::OLAPInternalError(OLAP_ERR_FUNC_NOT_IMPLEMENTED); }
    InvertedIndexReaderType type() override;

private:
};

class InvertedIndexVisitor : public lucene::util::bkd::bkd_reader::intersect_visitor {
private:
    roaring::Roaring* hits;
    uint32_t num_hits;
    bool only_count;
    //std::shared_ptr<lucene::util::BitSet> hits;
    lucene::util::bkd::bkd_reader* reader;
    InvertedIndexQueryType query_type;
public:
    std::string queryMin;
    std::string queryMax;
public:
    InvertedIndexVisitor(roaring::Roaring* hits,
                         InvertedIndexQueryType query_type, bool only_count = false);
    virtual ~InvertedIndexVisitor() = default;

    void set_reader(lucene::util::bkd::bkd_reader* r) { reader = r; };
    lucene::util::bkd::bkd_reader* get_reader() { return reader; };

    void visit(int rowID) override;
    void visit(roaring::Roaring &r) override;
    void visit(roaring::Roaring &&r) ;
    void visit(Roaring *docID, std::vector<uint8_t> &packedValue) ;
    bool matches(uint8_t* packedValue);

    void visit(int rowID, std::vector<uint8_t>& packedValue) override;
    void visit(lucene::util::bkd::bkd_docID_set_iterator *iter, std::vector<uint8_t> &packedValue) override;

    lucene::util::bkd::relation compare(std::vector<uint8_t>& minPacked,
                                        std::vector<uint8_t>& maxPacked) override;
    uint32_t get_num_hits() const {
        return num_hits;
    }
};

class BkdIndexReader : public InvertedIndexReader {
public:
    explicit BkdIndexReader(io::FileSystem* fs, const std::string& path, const uint32_t uniq_id);
    ~BkdIndexReader() override {
        if (compoundReader != nullptr) {
            compoundReader->close();
            delete compoundReader;
            compoundReader = nullptr;
        }
    }

    Status new_iterator(InvertedIndexParserType analyser_type,
                        InvertedIndexIterator** iterator) override;

    Status query(const std::string& column_name, const void* query_value,
                 InvertedIndexQueryType query_type, InvertedIndexParserType analyser_type,
                 roaring::Roaring* bit_map) override;
    Status try_query(const std::string& column_name, const void* query_value,
                 InvertedIndexQueryType query_type, InvertedIndexParserType analyser_type,
                 uint32_t* count) override;
    Status bkd_query(const std::string& column_name, const void* query_value,
                                     InvertedIndexQueryType query_type,
				     std::shared_ptr<lucene::util::bkd::bkd_reader>&& r,
                                     InvertedIndexVisitor* visitor);

    InvertedIndexReaderType type() override;
    Status get_bkd_reader(lucene::util::bkd::bkd_reader*& reader);
    //std::shared_ptr<lucene::util::bkd::bkd_reader> get_bkd_reader() const;
private:
    const TypeInfo* _type_info {};
    const KeyCoder* _value_key_coder {};
    DorisCompoundReader* compoundReader;
};

class InvertedIndexIterator {
public:
    InvertedIndexIterator(InvertedIndexParserType analyser_type, InvertedIndexReader* reader)
            : _reader(reader), _analyser_type(analyser_type) {}

    Status read_from_inverted_index(const std::string& column_name, const void* query_value,
                                    InvertedIndexQueryType query_type, uint32_t segment_num_rows, 
                                    roaring::Roaring* bit_map);
    Status try_read_from_inverted_index(const std::string& column_name, const void* query_value,
                                    InvertedIndexQueryType query_type, uint32_t* count);

    InvertedIndexParserType get_inverted_index_analyser_type() const;

    InvertedIndexReaderType get_inverted_index_reader_type() const;

private:
    InvertedIndexReader* _reader;
    std::string _column_name;
    InvertedIndexParserType _analyser_type;
};

} // namespace segment_v2
} // namespace doris

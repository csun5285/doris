#include "http/action/pad_segment_action.h"

#include <cstdlib>

#include "common/status.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/storage_policy.h"
#include "olap/tablet_schema.h"

namespace doris {

// TODO(cyx): support building local pad segment
Status build_remote_pad_segment(const std::string& resource_id, int64_t tablet_id,
                                const std::string& rowset_id, int segment_id) {
    auto fs = get_filesystem(resource_id);
    if (fs == nullptr) {
        return Status::NotFound("could not find fs with resource_id={}", resource_id);
    }
    auto path = BetaRowset::remote_segment_path(tablet_id, rowset_id, segment_id);
    io::FileWriterPtr file_writer;
    RETURN_IF_ERROR(fs->create_file(path, &file_writer));
    auto schema = std::make_shared<TabletSchema>();
    SegmentWriterOptions opts;
    SegmentWriter segment_writer(file_writer.get(), segment_id, schema, nullptr, nullptr, INT32_MAX,
                                 opts, nullptr);
    uint64_t segment_file_size, index_size;
    RETURN_IF_ERROR(segment_writer.finalize(&segment_file_size, &index_size));
    return file_writer->close();
}

void PadSegmentAction::handle(HttpRequest* req) {
    LOG(INFO) << "accept one request " << req->debug_string();
    auto& tablet_id_str = req->param("tablet_id");
    auto& rowset_id = req->param("rowset_id");
    auto& segment_id_str = req->param("segment_id");
    auto& resource_id = req->param("resource_id");
    int64_t tablet_id = 0;
    int segment_id = 0;
    try {
        tablet_id = std::stol(tablet_id_str);
        segment_id = std::stoi(segment_id_str);
    } catch (const std::exception&) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "invalid tablet_id or segment_id");
        return;
    }
    if (rowset_id.empty()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "invalid rowset_id");
        return;
    }
    auto st = build_remote_pad_segment(resource_id, tablet_id, rowset_id, segment_id);
    if (!st.ok()) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, st.to_json());
        return;
    }
    HttpChannel::send_reply(req, HttpStatus::OK, st.to_json());
}

} // namespace doris

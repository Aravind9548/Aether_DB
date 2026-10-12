// Direct gRPC smoke test for the AetherAdmin service (Status/ListKeys/KillSelf).
// Usage: admin_grpc_test <host:port> [--expect-standalone] [--kill]
//
// Default mode (against a running node with a couple of SET keys already in
// it via client_grpc_test or the text protocol): calls Status and ListKeys,
// prints the results, and independently recomputes std::hash<string>{}(key)
// % 16 for every returned key to confirm shard_id matches exactly.
//
// --kill: also calls KillSelf and confirms the RPC itself succeeds (the
// process is expected to disappear shortly after - this test does not wait
// around to observe that, a separate process-liveness check does).
#include <grpcpp/grpcpp.h>
#include "admin.grpc.pb.h"
#include <iostream>
#include <cstdlib>
#include <functional>

using aether::admin::AetherAdmin;
using aether::admin::Empty;
using aether::admin::StatusResponse;
using aether::admin::ListKeysResponse;
using aether::admin::KillSelfResponse;

static void fail(const std::string& msg) {
    std::cerr << "FAIL: " << msg << std::endl;
    std::exit(1);
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: admin_grpc_test <host:port> [--kill]" << std::endl;
        return 1;
    }
    std::string target = argv[1];
    bool do_kill = (argc > 2 && std::string(argv[2]) == "--kill");

    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    auto stub = AetherAdmin::NewStub(channel);

    grpc::ClientContext ctx1;
    Empty empty_req;
    StatusResponse status_resp;
    grpc::Status status = stub->Status(&ctx1, empty_req, &status_resp);
    if (!status.ok()) fail("Status RPC failed: " + status.error_message());
    std::cout << "PASS: Status -> role=" << status_resp.role()
              << " term=" << status_resp.term()
              << " commit=" << status_resp.commit_index()
              << " last_log=" << status_resp.last_log_index()
              << " leader=" << status_resp.leader_hint() << std::endl;

    grpc::ClientContext ctx2;
    ListKeysResponse keys_resp;
    status = stub->ListKeys(&ctx2, empty_req, &keys_resp);
    if (!status.ok()) fail("ListKeys RPC failed: " + status.error_message());
    std::cout << "PASS: ListKeys -> " << keys_resp.entries_size() << " key(s)" << std::endl;

    std::hash<std::string> hasher;
    for (const auto& entry : keys_resp.entries()) {
        int expected_shard = static_cast<int>(hasher(entry.key()) % 16);
        std::cout << "  key=" << entry.key() << " val=" << entry.val()
                  << " shard_id=" << entry.shard_id()
                  << " (independently computed: " << expected_shard << ")" << std::endl;
        if (entry.shard_id() != expected_shard) {
            fail("shard_id mismatch for key '" + entry.key() + "': engine said " +
                 std::to_string(entry.shard_id()) + ", independent hash%16 says " +
                 std::to_string(expected_shard));
        }
    }
    std::cout << "PASS: all shard_id values match independently computed hash(key) % 16" << std::endl;

    if (do_kill) {
        grpc::ClientContext ctx3;
        KillSelfResponse kill_resp;
        status = stub->KillSelf(&ctx3, empty_req, &kill_resp);
        if (!status.ok()) fail("KillSelf RPC failed: " + status.error_message());
        if (!kill_resp.acknowledged()) fail("KillSelf did not report acknowledged=true");
        std::cout << "PASS: KillSelf acknowledged (process should exit shortly)" << std::endl;
    }

    std::cout << "ALL ADMIN GRPC SMOKE TESTS PASSED" << std::endl;
    return 0;
}

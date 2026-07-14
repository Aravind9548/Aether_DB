// Direct gRPC smoke test for the client data-path service (Set/Get/Delete).
// Usage: client_grpc_test <host:port> [--expect-not-leader]
//
// Default mode: SET a key, GET it back and verify the value, DELETE it,
// GET again and verify it's gone. Exits 0 on success, 1 on any mismatch.
//
// --expect-not-leader mode: sends a single SET and asserts the response's
// not_leader flag is set with a non-empty leader_hint (used against a
// follower in cluster mode to verify the NOT_LEADER redirect path).
#include <grpcpp/grpcpp.h>
#include "client.grpc.pb.h"
#include <iostream>
#include <cstdlib>

using aether::client::AetherClient;
using aether::client::SetRequest;
using aether::client::SetResponse;
using aether::client::GetRequest;
using aether::client::GetResponse;
using aether::client::DeleteRequest;
using aether::client::DeleteResponse;

static void fail(const std::string& msg) {
    std::cerr << "FAIL: " << msg << std::endl;
    std::exit(1);
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: client_grpc_test <host:port> [--expect-not-leader]" << std::endl;
        return 1;
    }
    std::string target = argv[1];
    bool expect_not_leader = (argc > 2 && std::string(argv[2]) == "--expect-not-leader");

    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    auto stub = AetherClient::NewStub(channel);

    grpc::ClientContext ctx1;
    SetRequest set_req;
    set_req.set_key("smoke_test_key");
    set_req.set_val("smoke_test_val");
    SetResponse set_resp;
    grpc::Status status = stub->Set(&ctx1, set_req, &set_resp);
    if (!status.ok()) fail("SET RPC failed: " + status.error_message());

    if (expect_not_leader) {
        if (!set_resp.not_leader()) fail("expected not_leader=true, got false");
        if (set_resp.leader_hint().empty()) fail("expected non-empty leader_hint");
        std::cout << "PASS: NOT_LEADER redirect returned leader_hint=" << set_resp.leader_hint() << std::endl;
        return 0;
    }

    if (set_resp.not_leader()) fail("unexpected not_leader=true (leader_hint=" + set_resp.leader_hint() + ")");
    if (!set_resp.ok()) fail("SET did not return ok=true (error=" + set_resp.error() + ")");
    std::cout << "PASS: SET ok" << std::endl;

    grpc::ClientContext ctx2;
    GetRequest get_req;
    get_req.set_key("smoke_test_key");
    GetResponse get_resp;
    status = stub->Get(&ctx2, get_req, &get_resp);
    if (!status.ok()) fail("GET RPC failed: " + status.error_message());
    if (!get_resp.found()) fail("GET did not find the key we just SET");
    if (get_resp.val() != "smoke_test_val") fail("GET returned wrong value: " + get_resp.val());
    std::cout << "PASS: GET returned correct value" << std::endl;

    grpc::ClientContext ctx3;
    DeleteRequest del_req;
    del_req.set_key("smoke_test_key");
    DeleteResponse del_resp;
    status = stub->Delete(&ctx3, del_req, &del_resp);
    if (!status.ok()) fail("DELETE RPC failed: " + status.error_message());
    if (!del_resp.deleted()) fail("DELETE did not report deleted=true");
    std::cout << "PASS: DELETE ok" << std::endl;

    grpc::ClientContext ctx4;
    GetRequest get_req2;
    get_req2.set_key("smoke_test_key");
    GetResponse get_resp2;
    status = stub->Get(&ctx4, get_req2, &get_resp2);
    if (!status.ok()) fail("GET (post-delete) RPC failed: " + status.error_message());
    if (get_resp2.found()) fail("GET after DELETE still found the key");
    std::cout << "PASS: GET after DELETE correctly returns not found" << std::endl;

    std::cout << "ALL CLIENT GRPC SMOKE TESTS PASSED" << std::endl;
    return 0;
}

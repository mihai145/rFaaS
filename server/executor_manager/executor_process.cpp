
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <tuple>

#include <unistd.h>
#include <fcntl.h>
#include <spawn.h>
#include <sys/wait.h>

#include <spdlog/spdlog.h>

#include <rfaas/allocation.hpp>

#include "manager.hpp"
#include "executor_process.hpp"
#include "settings.hpp"
#include "../common.hpp"

extern "C" char **environ;

namespace rfaas::executor_manager {

  ActiveExecutor::~ActiveExecutor()
  {
    for(int i = 0; i < connections_len; ++i) {
      delete connections[i];
    }
    delete[] connections; 
  }

  void ActiveExecutor::add_executor(rdmalib::Connection* connection)
  {
    int pos = connections_len++;
    connections[pos] = connection;
  }

  ProcessExecutor::ProcessExecutor(int cores, ProcessExecutor::time_t alloc_begin, pid_t pid):
    ActiveExecutor(cores),
    _pid(pid)
  {
    _allocation_begin = alloc_begin;
    // FIXME: remove after connection
    _allocation_finished = _allocation_begin;
  }

  std::tuple<ProcessExecutor::Status,int> ProcessExecutor::check() const
  {
    int status = 0;
    if(_pid <= 0) // spawn failed
      return std::make_tuple(Status::FINISHED_FAIL, -1);

    // WNOHANG - returns immediately without waiting
    // WUNTRACED - return the status of stopped children
    pid_t return_pid = waitpid(_pid, &status, WNOHANG | WUNTRACED);

    if(!return_pid) {
      return std::make_tuple(Status::RUNNING, 0);
    } else {

      if(return_pid == -1 && errno == ECHILD) {
        return std::make_tuple(Status::FINISHED, -1);
      } else if (return_pid == -1) {
        // Unknown problem
        return std::make_tuple(Status::FINISHED_FAIL, -1);
      } else if(WIFEXITED(status)) {
        return std::make_tuple(Status::FINISHED, WEXITSTATUS(status));
      } else if (WIFSIGNALED(status)) {
        return std::make_tuple(Status::FINISHED_FAIL, WTERMSIG(status));
      } else {
        // Unknown problem
        return std::make_tuple(Status::FINISHED_FAIL, -1);
      }
    }
  }

  int ProcessExecutor::id() const
  {
    return static_cast<int>(_pid);
  }

  ProcessExecutor* ProcessExecutor::spawn(
    const rfaas::AllocationRequest & request,
    const ExecutorSettings & exec,
    const executor::ManagerConnection & conn,
    const Lease & lease
  )
  {
    auto begin = std::chrono::high_resolution_clock::now();
    //spdlog::info("Child fork begins work on PID {} req {}", mypid, fmt::ptr(&request));
    std::string client_addr{request.listen_address};
    std::string client_port = std::to_string(request.listen_port);
    //spdlog::error("Child fork begins work on PID {} req {}", mypid, fmt::ptr(&request));
    std::string client_in_size = std::to_string(request.input_buf_size);
    std::string client_func_size = std::to_string(request.func_buf_size);
    std::string client_cores = std::to_string(lease.cores);
    std::string client_timeout = std::to_string(request.hot_timeout);
    //spdlog::error("Child fork begins work on PID {}", mypid);
    std::string executor_repetitions = std::to_string(exec.repetitions);
    std::string executor_warmups = std::to_string(exec.warmup_iters);
    std::string executor_recv_buf = std::to_string(exec.recv_buffer_size);
    std::string executor_max_inline = std::to_string(exec.max_inline_data);
    std::string executor_pin_threads;
    if(exec.pin_threads >= 0)
      executor_pin_threads = std::to_string(0);
    else
      executor_pin_threads = std::to_string(exec.pin_threads);
    bool use_docker = exec.use_docker;

    std::string mgr_port = std::to_string(conn.port);
    std::string mgr_secret = std::to_string(conn.secret);
    std::string mgr_buf_addr = std::to_string(conn.r_addr);
    std::string mgr_buf_rkey = std::to_string(conn.r_key);

    std::string use_multiprocessing = "--use-multiprocessing=";
    use_multiprocessing += exec.use_multiprocessing ? "true" : "false";

    const char * argv_baremetal[] = {
      "executor",
      "-a", client_addr.c_str(),
      "-p", client_port.c_str(),
      "--polling-mgr", "thread",
      "-r", executor_repetitions.c_str(),
      "-x", executor_recv_buf.c_str(),
      "-s", client_in_size.c_str(),
      "--pin-threads", executor_pin_threads.c_str(),
      "--fast", client_cores.c_str(),
      "--warmup-iters", executor_warmups.c_str(),
      "--max-inline-data", executor_max_inline.c_str(),
      "--func-size", client_func_size.c_str(),
      "--timeout", client_timeout.c_str(),
      "--mgr-address", conn.addr.c_str(),
      "--mgr-port", mgr_port.c_str(),
      "--mgr-secret", mgr_secret.c_str(),
      "--mgr-buf-addr", mgr_buf_addr.c_str(),
      "--mgr-buf-rkey", mgr_buf_rkey.c_str(),
      use_multiprocessing.c_str(),
      nullptr
    };
    //const char * argv_docker[] = {
    //  "docker_rdma_sriov", "run",
    //  "--rm",
    //  "--net=mynet", "-i", //"-it",
    //  // FIXME: make configurable
    //  "--ip=148.187.105.220",
    //  // FIXME: make configurable
    //  "--volume", "/users/mcopik/projects/rdma/repo/build_repo2:/opt",
    //  // FIXME: make configurable
    //  "rdma-test",
    //  "/opt/bin/executor",
    //  "-a", client_addr.c_str(),
    //  "-p", client_port.c_str(),
    //  "--polling-mgr", "thread",
    //  "-r", executor_repetitions.c_str(),
    //  "-x", executor_recv_buf.c_str(),
    //  "-s", client_in_size.c_str(),
    //  "--pin-threads", "true",
    //  "--fast", client_cores.c_str(),
    //  "--warmup-iters", executor_warmups.c_str(),
    //  "--max-inline-data", executor_max_inline.c_str(),
    //  "--func-size", client_func_size.c_str(),
    //  "--timeout", client_timeout.c_str(),
    //  "--mgr-address", conn.addr.c_str(),
    //  "--mgr-port", mgr_port.c_str(),
    //  "--mgr-secret", mgr_secret.c_str(),
    //  "--mgr-buf-addr", mgr_buf_addr.c_str(),
    //  "--mgr-buf-rkey", mgr_buf_rkey.c_str(),
    //  nullptr
    //};
    const char * argv_docker[] = {
      "docker_rdma_sriov", "run",
      "--rm",
      "--net=mynet", "-i", //"-it",
      // FIXME: make configurable
      "--ip=148.187.105.250",
      // FIXME: make configurable
      "--volume", "/users/mcopik/projects/rdma/repo/build_repo2:/opt",
      // FIXME: make configurable
      "rdma-test",
      "/opt/bin/executor",
      "-a", client_addr.c_str(),
      "-p", client_port.c_str(),
      "--polling-mgr", "thread",
      "-r", executor_repetitions.c_str(),
      "-x", executor_recv_buf.c_str(),
      "-s", client_in_size.c_str(),
      "--pin-threads", executor_pin_threads.c_str(),
      "--fast", client_cores.c_str(),
      "--warmup-iters", executor_warmups.c_str(),
      "--max-inline-data", executor_max_inline.c_str(),
      "--func-size", client_func_size.c_str(),
      "--timeout", client_timeout.c_str(),
      "--mgr-address", conn.addr.c_str(),
      "--mgr-port", mgr_port.c_str(),
      "--mgr-secret", mgr_secret.c_str(),
      "--mgr-buf-addr", mgr_buf_addr.c_str(),
      "--mgr-buf-rkey", mgr_buf_rkey.c_str(),
      use_multiprocessing.c_str(),
      nullptr
    };
    const char * const * argv = use_docker ? argv_docker : argv_baremetal;

    // The executor used to name its log file after its own pid; create a tmpfile now
    static std::atomic<uint64_t> spawn_counter{0};
    std::string tmp_file = ".executor_pending_" + std::to_string(getpid())
      + "_" + std::to_string(spawn_counter++);

    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_addopen(
      &actions, STDOUT_FILENO, tmp_file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR
    );
    posix_spawn_file_actions_adddup2(&actions, STDOUT_FILENO, STDERR_FILENO);

    // the executor leads its own process group.
    posix_spawnattr_t attr;
    posix_spawnattr_init(&attr);
    posix_spawnattr_setflags(&attr, POSIX_SPAWN_SETPGROUP);
    posix_spawnattr_setpgroup(&attr, 0);

    pid_t mypid = -1;
    int ret = posix_spawnp(
      &mypid, argv[0], &actions, &attr, const_cast<char* const*>(argv), environ
    );
    posix_spawn_file_actions_destroy(&actions);
    posix_spawnattr_destroy(&attr);

    if(ret != 0) {
      spdlog::error("Spawning executor failed! {}", strerror(ret));
      unlink(tmp_file.c_str());
      mypid = -1;
    } else {
      auto out_file = ("executor_" + std::to_string(mypid));
      if(rename(tmp_file.c_str(), out_file.c_str()) == -1)
        spdlog::error("Failed to rename {} to {}: {}", tmp_file, out_file, strerror(errno));
    }

    // setpgid(mypid, mypid);  // both-sides setpgid
    return new ProcessExecutor{lease.cores, begin, mypid};
  }

}


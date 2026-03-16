
#include <iostream>
#include <unistd.h>

#include <spdlog/spdlog.h>
#include "worker_opts.hpp"

int main(int argc, char **argv)
{
  auto opts = worker::opts(argc, argv);
  spdlog::info(
      "Worker process started. "
      "Options received from executor: expecting function size {}, function payloads {},"
      " receive WCs buffer size {}, max inline data {}, hot polling timeout {}"
      "manager runs at {}:{}, its secret is {}, the accounting buffer is at {} with rkey {}",
      opts.functions_size, opts.buf_size, opts.recv_buffer_size, opts.max_inline_data,
      opts.timeout, opts.mgr_conn_addr, opts.mgr_conn_port, opts.mgr_conn_secret,
      opts.mgr_conn_r_addr, opts.mgr_conn_r_key);

  sleep(5);
  spdlog::info("Worker process ended");

  return 0;
}

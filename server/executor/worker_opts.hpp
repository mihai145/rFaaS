#ifndef __SERVER_WORKER_OPTS_HPP__
#define __SERVER_WORKER_OPTS_HPP__

#include <cxxopts.hpp>

namespace worker
{
    struct Options
    {
        std::string addr;
        int port;
        int id;
        int functions_size;
        int buf_size;
        int recv_buffer_size;
        int max_inline_data;
        std::string mgr_conn_addr;
        int mgr_conn_port;
        int mgr_conn_secret;
        uint64_t mgr_conn_r_addr;
        uint32_t mgr_conn_r_key;
        int timeout;
    };

    Options opts(int argc, char **argv)
    {
        // overlap with server::opts
        cxxopts::Options options("serverless-rdma-worker", "Handle functions invocations.");
        options.add_options()
            ("addr", "", cxxopts::value<std::string>())
            ("port", "", cxxopts::value<int>()->default_value("0"))
            ("id", "", cxxopts::value<int>()->default_value("0"))
            ("functions_size", "", cxxopts::value<int>()->default_value("0"))
            ("buf_size", "", cxxopts::value<int>()->default_value("0"))
            ("recv_buffer_size", "", cxxopts::value<int>()->default_value("0"))
            ("max_inline_data", "", cxxopts::value<int>()->default_value("0"))
            ("mgr_conn_addr", "", cxxopts::value<std::string>())
            ("mgr_conn_port", "", cxxopts::value<int>()->default_value("0"))
            ("mgr_conn_secret", "", cxxopts::value<int>()->default_value("0"))
            ("mgr_conn_r_addr", "", cxxopts::value<uint64_t>()->default_value("0"))
            ("mgr_conn_r_key", "", cxxopts::value<uint32_t>()->default_value("0"))
            ("timeout", "", cxxopts::value<int>()->default_value("0"))
        ;
        auto parsed_options = options.parse(argc, argv);

        Options result;
        result.addr = parsed_options["addr"].as<std::string>();
        result.port = parsed_options["port"].as<int>();
        result.id = parsed_options["id"].as<int>();
        result.functions_size = parsed_options["functions_size"].as<int>();
        result.buf_size = parsed_options["buf_size"].as<int>();
        result.recv_buffer_size = parsed_options["recv_buffer_size"].as<int>();
        result.max_inline_data = parsed_options["max_inline_data"].as<int>();
        result.mgr_conn_addr = parsed_options["mgr_conn_addr"].as<std::string>();
        result.mgr_conn_port = parsed_options["mgr_conn_port"].as<int>();
        result.mgr_conn_r_addr = parsed_options["mgr_conn_r_addr"].as<uint64_t>();
        result.mgr_conn_r_key = parsed_options["mgr_conn_r_key"].as<uint32_t>();
        result.timeout = parsed_options["timeout"].as<int>();

        return result;
    }
}

#endif

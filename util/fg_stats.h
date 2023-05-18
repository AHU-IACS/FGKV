#ifndef _FG_STATS_H_
#define _FG_STATS_H_

#include <stdint.h>

// struct LevelAndStage {
//     int level;
//     uint8_t* stage;

//     LevelAndStage() : level(-1), stage(NULL) { }
// };

struct FG_Stats {
    uint64_t fg_total_write;
    uint64_t total_compaction_read;

    uint64_t block_reader_r_count;
    uint64_t block_reader_r_time; 
    uint64_t block_reader_r_read_block_cache_count;
    uint64_t block_reader_r_read_block_count;
    uint64_t block_reader_r_read_block_time;
    uint64_t read_block_r_file_read_time;
    uint64_t read_block_r_file_read_size; 

    uint64_t block_reader_w_count;
    uint64_t block_reader_w_time; 
    uint64_t block_reader_w_read_block_cache_count;
    uint64_t block_reader_w_read_block_count;
    uint64_t block_reader_w_read_block_time;
    uint64_t read_block_w_file_read_time;
    uint64_t read_block_w_file_read_size;

    uint64_t write_block_count;
    uint64_t write_block_time;

    uint64_t fg_getchosenpositions_time;
    // uint64_t fg_apply_time;
    // uint64_t fg_saveto_time;

    uint64_t pick_compaction_time[6];
    uint32_t minor_compaction_count_in_l0;
    uint32_t minor_compaction_count_in_root;
    uint32_t minor_compaction_count_in_child;
    uint32_t l0_doc_count;
    uint32_t fg_root_compaction_count;
    uint32_t fg_child_compaction_count;

    uint64_t minor_compaction_time_in_l0;
    uint64_t minor_compaction_time_in_root;
    uint64_t minor_compaction_time_in_child;
    uint64_t l0_doc_time;
    // root分支的时间，包括根表合并中的minor compaction
    uint64_t fg_root_compaction_time;
    // !root分支的时间，包括子表合并中的minor compaction
    uint64_t fg_child_compaction_time;

    uint64_t mem_get_count;
    uint64_t mem_get_time;

    uint64_t table_cache_get_count;
    uint64_t table_cache_get_time;
    uint64_t find_table_time;
    uint64_t internal_get_count;
    uint64_t internal_get_time;
    uint64_t find_index_key_time;
    uint64_t internal_get_seek_data_block_time; 

    // 每层compaction通过TableBuilder写的block数
    uint64_t add_block_count[7];

    int chosen_child;
    uint64_t chosen_child_size;

    int total_child_number_in_cluster_compaction;
    int original_cluster_compaction_count;

    FG_Stats() : fg_total_write(0), 
                total_compaction_read(0), 

                block_reader_r_read_block_count(0), 
                block_reader_r_read_block_time(0), 
                block_reader_r_read_block_cache_count(0), 
                block_reader_r_count(0), 
                block_reader_r_time(0), 
                read_block_r_file_read_time(0), 
                read_block_r_file_read_size(0),
                
                block_reader_w_read_block_count(0), 
                block_reader_w_read_block_time(0), 
                block_reader_w_read_block_cache_count(0), 
                block_reader_w_count(0),
                block_reader_w_time(0), 
                read_block_w_file_read_time(0), 
                read_block_w_file_read_size(0),

                write_block_count(0), 
                write_block_time(0),  
                
                fg_getchosenpositions_time(0), 
                // fg_apply_time(0), 
                // fg_saveto_time(0), 

                pick_compaction_time{0},
                minor_compaction_count_in_l0(0), 
                minor_compaction_count_in_root(0), 
                minor_compaction_count_in_child(0), 
                l0_doc_count(0), 
                fg_root_compaction_count(0), 
                fg_child_compaction_count(0), 

                minor_compaction_time_in_l0(0), 
                minor_compaction_time_in_root(0), 
                minor_compaction_time_in_child(0), 
                l0_doc_time(0), 
                fg_root_compaction_time(0), 
                fg_child_compaction_time(0), 

                mem_get_count(0), 
                mem_get_time(0),

                table_cache_get_count(0), 
                table_cache_get_time(0), 
                find_table_time(0), 
                internal_get_count(0), 
                internal_get_time(0),
                find_index_key_time(0), 
                internal_get_seek_data_block_time(0), 
                
                add_block_count{0}, 
                chosen_child(0), 
                chosen_child_size(0), 
                
                total_child_number_in_cluster_compaction(0), 
                original_cluster_compaction_count(0) { }
    
    void Reset() { 
        fg_total_write = 0;
        total_compaction_read = 0;

        block_reader_r_read_block_count = 0;
        block_reader_r_read_block_time = 0;
        block_reader_r_read_block_cache_count = 0;
        block_reader_r_count = 0;
        block_reader_r_time = 0;
        read_block_r_file_read_time = 0; 
        read_block_r_file_read_size = 0;

        block_reader_w_read_block_count = 0;
        block_reader_w_read_block_time = 0;
        block_reader_w_read_block_cache_count = 0;
        block_reader_w_count = 0;
        block_reader_w_time = 0;
        read_block_w_file_read_time = 0; 
        read_block_w_file_read_size = 0;

        write_block_count = 0;
        write_block_time = 0;
                
        fg_getchosenpositions_time = 0;
        // fg_apply_time = 0;
        // fg_saveto_time = 0;

        minor_compaction_count_in_l0 = 0;
        minor_compaction_count_in_root = 0;
        minor_compaction_count_in_child = 0;
        l0_doc_count = 0;
        fg_root_compaction_count = 0;
        fg_child_compaction_count = 0;

        minor_compaction_time_in_l0 = 0;
        minor_compaction_time_in_root = 0;
        minor_compaction_time_in_child = 0;
        l0_doc_time = 0;
        fg_root_compaction_time = 0;
        fg_child_compaction_time = 0;

        mem_get_count = 0;
        mem_get_time = 0;

        table_cache_get_count = 0;
        table_cache_get_time = 0;
        find_table_time = 0;
        internal_get_count = 0;
        internal_get_time = 0;
        find_index_key_time = 0;
        internal_get_seek_data_block_time = 0;

        for (int i = 0; i < 6; i ++) {
            pick_compaction_time[i] = 0;
        }

        for (int i = 0; i < 7; i ++) {
            add_block_count[i] = 0;
        }

        chosen_child = 0;
        chosen_child_size = 0;

        total_child_number_in_cluster_compaction;
        original_cluster_compaction_count = 0;

    }
};


#endif
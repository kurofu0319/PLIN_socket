//
// Copyright (c) Zhou Zhang.
// Implementation of PLIN index structure
//

#pragma once

#include "inner_node.h"
#include "piecewise_linear_model.h"
#include <sys/stat.h>
#include <cstdio>
//#include <omp.h>

// #include <libpmemobj.h>
// #include <libpmem.h>

#include "flush.h"
#include "spinlock.h"
#include "newallocator.h"


// #include "pmallocator.h"



class PlinIndex
{
    typedef PlinIndex SelfType;
    // For storing models, to build nodes
    struct Segment
    {
        _key_t first_key;
        double slope;
        double intercept;
        uint64_t number;
        explicit Segment(const typename OptimalPiecewiseLinearModel<_key_t, size_t>::CanonicalSegment &cs)
            : first_key(cs.get_first_x()),
              number(cs.get_number())
        {
            auto [cs_slope, cs_intercept] = cs.get_floating_point_segment(first_key);
            slope = cs_slope;
            intercept = cs_intercept;
        }
    };

    struct split_log
    {
        LeafNode *leaf_to_split = NULL;
        LeafNode *left_sibling = NULL;
        LeafNode *right_sibling = NULL;
        LeafNode *left_node = NULL;
        LeafNode *right_node = NULL;
        // locked[0]: write lock, locked[1]: valid, locked[2]: orphan
        volatile uint32_t locked = 0;

        inline bool try_get_lock()
        {
            uint32_t new_value = 0;
            uint32_t old_value = __atomic_load_n(&locked, __ATOMIC_ACQUIRE);
            if (!(old_value & 1u))
            {
                new_value = old_value | 1u;
                return CAS(&locked, &old_value, new_value);
            }
            return false;
        }

        inline bool check_lock()
        {
            uint32_t v = __atomic_load_n(&locked, __ATOMIC_ACQUIRE);
            if (v & 1u)
            {
                return false;
            }
            return true;
        }

        inline bool check_valid()
        {
            uint32_t v = __atomic_load_n(&locked, __ATOMIC_ACQUIRE);
            if (v & 2u)
            {
                return true;
            }
            return false;
        }

        inline bool check_orphan()
        {
            uint32_t v = __atomic_load_n(&locked, __ATOMIC_ACQUIRE);
            if (v & 4u)
            {
                return true;
            }
            return false;
        }

        inline void set_valid()
        {
            ADD(&locked, 2);
        }

        inline void set_orphan()
        {
            ADD(&locked, 4);
        }

        inline void release_lock()
        {
            __atomic_store_n(&locked, 0, __ATOMIC_RELEASE);
        }
    };

    struct plin_metadata
    {
        _key_t min_key = 0;
        _key_t max_key = 0;
        // void *left_buffer = NULL;
        // void *right_buffer = NULL;
        btree::map<_key_t, _payload_t> *left_buffer = nullptr;
        btree::map<_key_t, _payload_t> *right_buffer = nullptr;
        uint32_t leaf_number = 0;
        uint32_t orphan_number = 0;
        uint32_t left_buffer_number = 0;
        uint32_t right_buffer_number = 0;
        uint32_t root_number = 0;
        uint32_t level = 0;
        // Used for rebuilding, no SMO in the process of rebuilding, 1 bit lock & 31 bit read counter
        volatile uint32_t smo_lock = 0;
        uint32_t global_version = 0;
        uint32_t rebuilding = 0;
        plin_metadata *old_plin_ = NULL;
        InnerSlot roots[ROOT_SIZE];
        split_log logs[LOG_NUMBER];
        // std::vector<LeafNode*> *leaf_nodes = nullptr;

        std::vector<InnerSlot*> *last_slots = nullptr;
        // InnerSlot* last_slots = nullptr;

        inline bool get_write_lock()
        {
            uint32_t v;
            do
            {
                v = __atomic_load_n(&smo_lock, __ATOMIC_ACQUIRE);
                if (v & lockSet)
                    return false;
            } while (!CAS(&smo_lock, &v, v | lockSet));

            // Wait until the readers all exit the critical section
            do
            {
                v = __atomic_load_n(&smo_lock, __ATOMIC_ACQUIRE);
            } while (v & lockMask);
            return true;
        }

        inline bool try_get_read_lock()
        {
            uint32_t v;
            do
            {
                v = __atomic_load_n(&smo_lock, __ATOMIC_ACQUIRE);
                if (v & lockSet)
                    return false;
            } while (!CAS(&smo_lock, &v, v + 1));
            return true;
        }

        inline void release_read_lock() { SUB(&smo_lock, 1); }

        inline void release_write_lock()
        {
            __atomic_store_n(&smo_lock, 0, __ATOMIC_RELEASE);
        }
    };

    plin_metadata *plin_;

public:
    std::chrono::nanoseconds split_t, rebuild_t, log_t;
    uint32_t split_times = 0;
    uint32_t rebuild_times = 0;

    PlinIndex(std::string id = "plin", bool recovery = false)
    {
        if (!recovery)
        {
            
            // galc = new PMAllocator(path.c_str(), false, id.c_str());
            
            void* allocated_memory = malloc(sizeof(plin_metadata));

            plin_ = new (allocated_memory) plin_metadata;        
            // new btree(&plin_->left_buffer, true);
            // new btree(&plin_->right_buffer, true);
            plin_->left_buffer = new btree::map<_key_t, _payload_t>();
            plin_->right_buffer = new btree::map<_key_t, _payload_t>();
            // plin_->leaf_nodes = new std::vector<LeafNode*>();
            plin_->last_slots = new std::vector<InnerSlot*>();
            // std::cout << "init Plin" << std::endl;
            // do_flush(plin_, sizeof(plin_metadata));
            mfence();
        }
        // else {
        //     std::cout << "rebuild root" << std::endl;
        //     // galc = new PMAllocator(path.c_str(), true, id.c_str());
        //     // plin_ = (plin_metadata *) galc->get_root(sizeof(plin_metadata));
        //     // plin_->global_version++;
        //     // if (plin_->rebuilding > 0) {
        //     //     plin_metadata * old_plin_ = galc->absolute(plin_->old_plin_);
        //     //     if (plin_->rebuilding > 1) {
        //     //         plin_->root_number = old_plin_->root_number;
        //     //         for (uint32_t i = 0; i < plin_->root_number; ++i)
        //     //             plin_->roots[i] = old_plin_->roots[i];
        //     //         for (uint32_t i = plin_->root_number; i < ROOT_SIZE; ++i)
        //     //             plin_->roots[i].min_key = FREE_FLAG;
        //     //         plin_->level = old_plin_->level;
        //     //         plin_->orphan_number = old_plin_->orphan_number;
        //     //     }
        //     //     plin_->rebuilding = 0;
        //     //     galc->free(old_plin_);
        //     //     plin_->old_plin_ = NULL;
        //     //     do_flush(plin_, sizeof(plin_metadata));
        //     //     mfence();
        //     // }
        //     // #ifdef BACKGROUND_CHECKLOGS
        //     //     std::thread check_logs_thread(&SelfType::check_logs, this);
        //     //     check_logs_thread.detach();
        //     // #else
        //     //     check_logs();
        //     // #endif
        // }
    }
    

    void check_logs()
    {
        for (uint32_t i = 0; i < LOG_NUMBER; ++i)
        {
            if (!plin_->logs[i].check_lock())
            {
                LeafNode *left_sibling = plin_->logs[i].left_sibling;
                LeafNode *right_sibling = plin_->logs[i].right_sibling;
                if (plin_->logs[i].check_valid())
                {
                    LeafNode *left_node = plin_->logs[i].left_node;
                    LeafNode *right_node = plin_->logs[i].right_node;
                    if (left_sibling)
                    {
                        left_sibling->set_next(left_node);
                    }
                    if (right_sibling)
                    {
                        right_sibling->set_prev(right_node);
                    }
                    if (!plin_->logs[i].check_orphan())
                    {
                        _key_t first_key;
                        InnerSlot accelerator;
                        left_node->get_info(first_key, accelerator);
                        upsert_node(accelerator);
                    }
                }
                else
                {
                    LeafNode *leaf_to_split = plin_->logs[i].leaf_to_split;
                    leaf_to_split->release_lock();
                    if (!plin_->logs[i].check_orphan())
                    {
                        uint32_t i = 0;
                        _key_t key = leaf_to_split->get_min_key();
                        while (i < plin_->root_number && key >= plin_->roots[i].min_key)
                        {
                            ++i;
                        }
                        InnerSlot *accelerator = &plin_->roots[--i];
                        if (accelerator->type())
                        {
                            accelerator = reinterpret_cast<InnerNode *>(accelerator->ptr)->find_leaf_node(key, accelerator);
                        }
                        accelerator->release_lock();
                    }
                }
                if (left_sibling)
                {
                    left_sibling->release_lock();
                }
                if (right_sibling)
                {
                    right_sibling->release_lock();
                }
                plin_->logs[i].release_lock();
            }
        }
        plin_->release_write_lock();
    }

    ~PlinIndex() {}

    uint32_t get_level() {
        return plin_->level;
    }

    void destroy()
    {
        LeafNode *cur = get_leftmost_leaf();
        LeafNode *next = cur->get_next();
        cur->destroy();
        while (next)
        {
            cur = next;
            next = cur->get_next();
            cur->destroy();
        }
        for (uint32_t i = 0; i < plin_->root_number; i++)
        {
            ((InnerNode *)(plin_->roots[i].ptr))->destroy();
        }
    }

    void destory_leaf()
    {
        LeafNode *cur = get_leftmost_leaf();
        LeafNode *next = cur->get_next();
        cur->destroy();
        while (next)
        {
            cur = next;
            next = cur->get_next();
            cur->destroy();
        }
    }

    void PrintInfo()
    {   
        std::cout << "Plin Info: " << std::endl;

        std::cout << "level: " << plin_->level << std::endl;
        std::cout << "leaf number: " << plin_->leaf_number << std::endl;
        std::cout << "orphan node number: " << plin_->orphan_number << std::endl;
        std::cout << "max key: " << plin_->max_key << std::endl;
        std::cout << "min key: " << plin_->min_key << std::endl;

        int i;
        
        for (i = 0; i < plin_->root_number; i++)
        {
            InnerSlot* acclerator = &plin_->roots[i];
            
            if (acclerator->type())
            {
                std::cout << "root: " << i << " slope: " << acclerator->slope << " intercept: " << acclerator->intercept << std::endl;
                (reinterpret_cast<InnerNode *>(acclerator->ptr))->traverseAllInnerSlots([](const InnerSlot& slot) {
                    // std::cout <<"Slope: " << slot.slope << ", Intercept: " << slot.intercept << ", min key: " << slot.min_key << std::endl;
                });
            }
        }
    }

    

    

    // Serialize entire PlinIndex
    void serializePlinIndex(std::vector<char>& buffer) 
    {
        // Serialize metadata
        size_t metaSize = sizeof(plin_->root_number) + sizeof(plin_->min_key) + sizeof(plin_->max_key) + sizeof(plin_->level); // etc.
        buffer.resize(metaSize);
        size_t offset = 0;
        memcpy(buffer.data(), &plin_->root_number, sizeof(plin_->root_number));
        offset += sizeof(plin_->root_number);
        memcpy(buffer.data() + offset, &plin_->min_key, sizeof(plin_->min_key));
        offset += sizeof(plin_->min_key);
        memcpy(buffer.data() + offset, &plin_->max_key, sizeof(plin_->max_key));
        offset += sizeof(plin_->max_key);
        memcpy(buffer.data() + offset, &plin_->level, sizeof(plin_->level));

        // Serialize roots and logs
        for (int i = 0; i < plin_->root_number; ++i) {
            InnerSlot* acclerator = &plin_->roots[i];
            serializeInnerSlot(*acclerator, buffer);


            // std::cout << "serializeInnerSlot " << std::endl;

            if (acclerator->type()) {
                // std::cout << "serializeInnerNode " << std::endl;
                (reinterpret_cast<InnerNode *>(acclerator->ptr))->serializeInnerNode(buffer);
            }
                
        }
    }

    void deserializePlinIndex(const std::vector<char>& buffer) 
    {
        void* allocated_memory = malloc(sizeof(plin_metadata));

        plin_ = new (allocated_memory) plin_metadata; 

        size_t offset = 0;
        if (offset + sizeof(plin_->root_number) <= buffer.size()) {
            memcpy(&plin_->root_number, buffer.data() + offset, sizeof(plin_->root_number));
            offset += sizeof(plin_->root_number);
        }

        if (offset + sizeof(plin_->min_key) <= buffer.size()) {
            memcpy(&plin_->min_key, buffer.data() + offset, sizeof(plin_->min_key));
            offset += sizeof(plin_->min_key);
        }

        if (offset + sizeof(plin_->max_key) <= buffer.size()) {
            memcpy(&plin_->max_key, buffer.data() + offset, sizeof(plin_->max_key));
            offset += sizeof(plin_->max_key);
        }

        if (offset + sizeof(plin_->max_key) <= buffer.size()) {
            memcpy(&plin_->level, buffer.data() + offset, sizeof(plin_->level));
            offset += sizeof(plin_->level);
        }

        

        // 反序列化 roots
        // std::cout << "root number: " << plin_->root_number << std::endl;
        
        for (int i = 0; i < plin_->root_number && offset < buffer.size(); ++i) {
            InnerSlot* accelerator = &plin_->roots[i];
            deserializeInnerSlot(*accelerator, buffer, offset);
            // std::cout << "root: " << i << " slope: " << accelerator->slope << " intercept: " << accelerator->intercept << std::endl;

            uint32_t block_number;
            memcpy(&block_number, buffer.data() + offset + 12, sizeof(block_number));
            // std::cout << "block number: " << block_number << std::endl;



            // 如果是 InnerNode，需要进一步反序列化
            
            if (accelerator->type()) {
                // std::cout << "deserialize inner node" << std::endl;

                // size_t slots_size = accelerator->block_number() * BLOCK_SIZE + sizeof(InnerNode);

                // InnerNode* inner_node = static_cast<InnerNode*>(::operator new(slots_size));
                uint64_t node_size_in_byte = block_number * BLOCK_SIZE + NODE_HEADER_SIZE;

                void* allocated_memory = malloc(node_size_in_byte);
                accelerator->ptr = new (allocated_memory) InnerNode(buffer, offset);
                // accelerator->ptr = inner_node;
                // (reinterpret_cast<InnerNode *>(accelerator->ptr))->deserializeInnerNode(buffer, offset);
            }
        }
    }

    void bulk_load(_key_t *keys, _payload_t *payloads, uint64_t number)
    {
        // Make segmentation for leaves
        std::vector<Segment> segments;
        auto in_fun = [keys](auto i)
        {
            return std::pair<_key_t, size_t>(keys[i], i);
        };
        auto out_fun = [&segments](auto cs)
        { segments.emplace_back(cs); };
        uint64_t last_n = make_segmentation(number, EPSILON_LEAF_NODE, in_fun, out_fun);

        // std::cout<< last_n << std::endl;

        // Build leaf nodes
        uint64_t start_pos = 0;
        auto first_keys = new _key_t[last_n];
        auto leaf_nodes = new LeafNode*[last_n];
        auto accelerators = new InnerSlot[last_n];
        // plin_->last_slots = new InnerSlot[last_n];
        LeafNode *prev = NULL;
        uint64_t i;
        for (i = 0; i < last_n; ++i)
        {
            uint64_t block_number = (uint64_t)segments[i].number / (LEAF_NODE_INIT_RATIO * LeafNode::LeafRealSlotsPerBlock) + 3;
            uint64_t node_size_in_byte = block_number * BLOCK_SIZE + NODE_HEADER_SIZE;
            first_keys[i] = segments[i].first_key;
            
            //std::cout << "0000" << std::endl;
            //std::cout << "i: " << i << std::endl;

            void* allocated_memory = malloc(node_size_in_byte);
            
            leaf_nodes[i] = ( new (allocated_memory) LeafNode(accelerators[i], block_number, keys, payloads, segments[i].number, start_pos, segments[i].slope, segments[i].intercept, plin_->global_version, prev, nullptr, i));
            
            
            // (*plin_->leaf_nodes)[i]->set_Slot(&accelerators[i]);
            

            // printf("%f\n", leaf_nodes[i]->get_min_key());
            

            //std::cout << "1111" << std::endl;

            
            
            start_pos += segments[i].number;


            
            prev = leaf_nodes[i];
        }

        //std::cout << "leaf successful" << std::endl;

        for (uint64_t i = 0; i < last_n - 1; ++i)
        {
            leaf_nodes[i]->set_next(leaf_nodes[i + 1]);
        }

        plin_->leaf_number = last_n;

        // std::cout << "leaf number: " << last_n << std::endl;

        // delete[] leaf_nodes;

        // for (size_t i = 0; i < last_n; ++ i) {
        //     plin_->last_slots->push_back(&accelerators[i]);
        // }

        // Build inner nodes recursively
        uint32_t level = 0;
        uint64_t offset = 0;
        auto accelerators_tmp = accelerators;
        auto first_keys_tmp = first_keys;
        auto in_fun_rec = [&first_keys](auto i)
        {
            return std::pair<_key_t, size_t>(first_keys[i], i);
        };
        while (level == 0 || last_n > ROOT_SIZE)
        {
            level++;
            offset += last_n;
            last_n = make_segmentation(last_n, EPSILON_INNER_NODE, in_fun_rec, out_fun);

            start_pos = 0;
            first_keys = new _key_t[last_n];
            accelerators = new InnerSlot[last_n];
            for (uint64_t i = 0; i < last_n; ++i)
            {
                uint64_t block_number = (uint64_t)segments[offset + i].number / (INNER_NODE_INIT_RATIO * InnerNode::InnerSlotsPerBlock) + EPSILON_INNER_NODE / InnerNode::InnerSlotsPerBlock + 3;
                uint64_t node_size_in_byte = block_number * BLOCK_SIZE + NODE_HEADER_SIZE;
                first_keys[i] = segments[offset + i].first_key;

                void* allocated_memory = malloc(node_size_in_byte);
                if (level == 1)
                    new (allocated_memory) InnerNode(accelerators[i], leaf_nodes, block_number, first_keys_tmp, accelerators_tmp, segments[offset + i].number, start_pos, segments[offset + i].slope, segments[offset + i].intercept, level, plin_->last_slots);
                else
                    new (allocated_memory) InnerNode(accelerators[i], nullptr, block_number, first_keys_tmp, accelerators_tmp, segments[offset + i].number, start_pos, segments[offset + i].slope, segments[offset + i].intercept, level);
                start_pos += segments[offset + i].number;

            
            }
            // std::cout << "slot size: " << plin_->last_slots->size() << std::endl;
            
            delete[] accelerators_tmp;
            delete[] first_keys_tmp;
            accelerators_tmp = accelerators;
            first_keys_tmp = first_keys;
        }

        delete[] leaf_nodes;

        // Build root

        // std::cout << "Build root" << std::endl;

        plin_->min_key = keys[0];
        plin_->max_key = keys[number - 1];
        plin_->root_number = last_n;
        plin_->level = level;
        plin_->global_version = 0;
        for (uint32_t i = 0; i < ROOT_SIZE; ++i)
        {
            plin_->roots[i].min_key = FREE_FLAG;
        }
        for (uint32_t i = 0; i < last_n; ++i)
        {
            plin_->roots[i] = accelerators_tmp[i];
        }
        //do_flush(plin_, sizeof(plin_metadata));
        mfence();
        delete[] accelerators_tmp;
        delete[] first_keys_tmp;
    }



    bool find(_key_t key, _payload_t &payload)
    {
        if (key >= plin_->min_key && key <= plin_->max_key)
        {
            do
            {
                uint32_t i = 0;
                while (i < plin_->root_number && key >= plin_->roots[i].min_key)
                {
                    ++i;
                }

                InnerSlot *accelerator = &plin_->roots[--i];
                if (accelerator->type())
                {
                    //std::cout << "find_leaf_node" << std::endl;
                    accelerator = (reinterpret_cast<InnerNode *>(accelerator->ptr))->find_leaf_node(key, accelerator);
                }
                if (accelerator->check_read_lock())
                {
                    //std::cout << "find" << std::endl;
                    uint32_t ret = (reinterpret_cast<LeafNode *>(accelerator->ptr))->find(key, payload, plin_->global_version, accelerator);
                    if (ret == 1)
                    {
                        return true;
                    }
                    else if (ret == 0)
                    {
                        return false;
                    }
                }
            } while (true);
        }
        // Find in buffer
        // else if (key < plin_->min_key)
        // {
        //     // std::cout << "Find in left buffer " << std::endl;
        //     btree *left_buffer = new btree(&plin_->left_buffer, false);
        //     return left_buffer->find(key, payload);
        // }
        // else
        // {
        //     // std::cout << "Find in right buffer " << std::endl;
        //     btree *right_buffer = new btree(&plin_->right_buffer, false);
        //     return right_buffer->find(key, payload);
        // }

        if (key < plin_->min_key) {
            // Find in left buffer
            auto it = plin_->left_buffer->find(key);
            if (it != plin_->left_buffer->end()) {
                payload = it->second;
                return true;
            }
            return false;
        } else if (key > plin_->max_key) {
            // Find in right buffer
            auto it = plin_->right_buffer->find(key);
            if (it != plin_->right_buffer->end()) {
                payload = it->second;
                return true;
            }
            return false;
        }
    }

    void find_Path(_key_t key, std::vector<int>& leaf_path)
    {
        if (key >= plin_->min_key && key <= plin_->max_key) {

            // std::cout << "find path" << std::endl;

            uint32_t i = 0;
            
            while (i < plin_->root_number && key >= plin_->roots[i].min_key)
            {
                
                ++i;
            }
                
            InnerSlot *accelerator = &plin_->roots[--i];

            // std::cout << "min key: " << plin_->min_key << std::endl; 
            // std::cout << "root number: " << plin_->root_number << std::endl;
            // std::cout << "find path" << std::endl;
            // std::cout << "root: " << i << std::endl; 

            // leaf_path.push_back(i);

            // std::cout << "push_back: "<< i << std::endl;

            if (accelerator->type()) {
                (reinterpret_cast<InnerNode *>(accelerator->ptr))->get_Leaf_path(key, accelerator, leaf_path);
                
            }
            else {
                leaf_path.push_back(accelerator->leaf_number);
                // std::cout << "leaf number: " << accelerator->leaf_number << std::endl;
            }
        }   
        else {
            // std::cout << "out of range" << std::endl;
            leaf_path.push_back(-1);
        }
    }

    _payload_t find_Payload(_key_t key, uint32_t leaf_number) {
        // std::cout << "find_Payload" << std::endl;
        _payload_t ans;

        do {
            InnerSlot* accelerator = plin_->last_slots->at(leaf_number);
            if (accelerator->check_read_lock())
            {
                // std::cout << "find" << std::endl;
                uint32_t ret = (reinterpret_cast<LeafNode *>(accelerator->ptr))->find(key, ans, plin_->global_version, accelerator);
                // std::cout << "ret" << ret << std::endl;
                // uint32_t ret = leaf_node->find(key, ans, plin_->global_version, accelerator);
                if (ret == 1)
                {
                    // std::cout << "ret: 1, Found!" << std::endl;
                }
                else if (ret == 0)
                {
                    std::cout << "ret: 0, not Found!" << std::endl;
                }

                return ans;
            }
        } while (true);
    }

    void range_query(_key_t lower_bound, _key_t upper_bound, std::vector<std::pair<_key_t, _payload_t>> &answers)
    {
        if (lower_bound < plin_->min_key) {
            auto it = plin_->left_buffer->lower_bound(lower_bound);
            auto end = plin_->left_buffer->upper_bound(std::min(upper_bound, plin_->min_key));
            for (; it != end; ++it) {
                answers.push_back({it->first, it->second});
            }
            // Adjust lower bound for the main range query if needed
            lower_bound = plin_->min_key;
        }

        // Handle right buffer
        if (upper_bound > plin_->max_key) {
            auto it = plin_->right_buffer->lower_bound(std::max(lower_bound, plin_->max_key));
            auto end = plin_->right_buffer->upper_bound(upper_bound);
            for (; it != end; ++it) {
                answers.push_back({it->first, it->second});
            }
            // Adjust upper bound for the main range query if needed
            upper_bound = plin_->max_key;
        }

        if (upper_bound >= plin_->min_key && lower_bound <= plin_->max_key)
        {
            uint32_t i = 0;
            while (i < plin_->root_number && lower_bound >= plin_->roots[i].min_key)
            {
                ++i;
            }
            InnerSlot *accelerator = &plin_->roots[--i];
            if (accelerator->type())
            {
                accelerator = reinterpret_cast<InnerNode *>(accelerator->ptr)->find_leaf_node(lower_bound, accelerator);
            }
            reinterpret_cast<LeafNode *>(accelerator->ptr)->range_query(lower_bound, upper_bound, answers, 0, accelerator);
        }
    }

    void range_query_Path(_key_t lower_bound, _key_t upper_bound, std::vector<std::pair<_key_t, _payload_t>> &answers, uint32_t leaf_number)
    {
        if (lower_bound < plin_->min_key) {
            auto it = plin_->left_buffer->lower_bound(lower_bound);
            auto end = plin_->left_buffer->upper_bound(std::min(upper_bound, plin_->min_key));
            for (; it != end; ++it) {
                answers.push_back({it->first, it->second});
            }
            // Adjust lower bound for the main range query if needed
            lower_bound = plin_->min_key;
        }

        // Handle right buffer
        if (upper_bound > plin_->max_key) {
            auto it = plin_->right_buffer->lower_bound(std::max(lower_bound, plin_->max_key));
            auto end = plin_->right_buffer->upper_bound(upper_bound);
            for (; it != end; ++it) {
                answers.push_back({it->first, it->second});
            }
            // Adjust upper bound for the main range query if needed
            upper_bound = plin_->max_key;
        }

        if (upper_bound >= plin_->min_key && lower_bound <= plin_->max_key)
        {
            InnerSlot* accelerator = plin_->last_slots->at(leaf_number);
            reinterpret_cast<LeafNode *>(accelerator->ptr)->range_query(lower_bound, upper_bound, answers, 0, accelerator);
        }
    }



    void upsert_Path(_key_t key, _payload_t payload, uint32_t leaf_number) {

        

        if (key >= plin_->min_key && key <= plin_->max_key) {
            uint32_t ret;
            do {

            _payload_t ans;
            // InnerSlot *accelerator = &plin_->roots[leaf_path[cur_ptr]];

            // for (int i = 1; i <= level; i ++ )
            //     accelerator = (reinterpret_cast<InnerNode *>(accelerator->ptr))->get_Slot(leaf_path[cur_ptr + i]);
                // LeafNode* inner_node = (*plin_->leaf_nodes)[leaf_number];

                InnerSlot* accelerator = plin_->last_slots->at(leaf_number);
                

                // uint32_t i = 0;
                // while (i < plin_->root_number && key >= plin_->roots[i].min_key)
                // {
                //     ++i;
                // }
                // InnerSlot *accelerator = &plin_->roots[--i];

                // if (accelerator->type())
                // {
                //     accelerator = reinterpret_cast<InnerNode *>(accelerator->ptr)->find_leaf_node(key, accelerator);
                // }

                LeafNode *leaf_to_split;
                // ret = 1 : update in a slot; ret = 2 : insert in a free slot; ret = 3 : update in overflow block; ret = 4 : insert in overflow block;
                // ret = 5 : insert in overflow block & need to split; ret = 6 : insert in overflow block & need to split orphan node; ret = 7 : the node is locked
                ret = reinterpret_cast<LeafNode *>(accelerator->ptr)->upsert(key, payload, plin_->global_version, leaf_to_split, accelerator, true);
                // ret = inner_node->upsert(key, payload, plin_->global_version, leaf_to_split, NULL);
                // Split leaf node
                if (ret == 5)
                {
                    #ifdef BACKGROUND_SPLIT
                        std::thread split_thread(&SelfType::split, this, leaf_to_split, nullptr);
                        split_thread.detach();
                    #else
                        // std::cout << "split 5" << std::endl;
                        // std::cout << "key: " << key << " acclerator min key: " << accelerator->min_key << std::endl;
                        // if (accelerator->leaf_number != plin_->last_slots->at(leaf_number)->leaf_number) {
                        //     std::cout << "before upsert: " << accelerator->leaf_number << " after upsert: " << plin_->last_slots->at(leaf_number)->leaf_number << std::endl;
                        // }

                        // std::cout << std::endl;
                        // std::cout << "before split last slot status: " << std::endl;
                        // for(InnerSlot* slot : *(plin_->last_slots)) {
                        //     std::cout << "leaf number: " << slot->leaf_number << std::endl;
                        // }
                        // std::cout << std::endl;
                        
                        if (leaf_to_split->get_Slot()->leaf_number != 10000) {
                            // std::cout << "change last slots: " << leaf_to_split->get_Slot()->leaf_number << std::endl;
                            // std::cout << "original leaf number: " << leaf_number << std::endl;
                            // leaf_number = leaf_to_split->get_Slot()->leaf_number;
                            // std::cout << "changed leaf number: " << leaf_number << std::endl;
                            split_Path(leaf_to_split, leaf_to_split->get_Slot(), plin_->last_slots);
                        }
                        else {
                            // std::cout << "split 10000" << std::endl;
                            split_Path(leaf_to_split, leaf_to_split->get_Slot(), plin_->last_slots);
                        }
                            
                    #endif
                }
                else if (ret == 6)
                {
#ifdef BACKGROUND_SPLIT
                        
                    std::thread split_thread(&SelfType::split, this, leaf_to_split, nullptr);
                    split_thread.detach();
#else
                    // std::cout << "split 6" << std::endl;
                    split(leaf_to_split, NULL);
#endif
                }
            

                    
        
            
            } while (ret == 7);
        }
        
        else if (key < plin_->min_key) {
            // Upsert in left buffer
            auto ret = plin_->left_buffer->insert({key, payload});
            if (!ret.second) { // key already exists, update the value
                ret.first->second = payload;
            }
        } else {
            // Upsert in right buffer
            auto ret = plin_->right_buffer->insert({key, payload});
            if (!ret.second) { // key already exists, update the value
                ret.first->second = payload;
            }
        }

        
    }



    void upsert(_key_t key, _payload_t payload)
    {
        if (key >= plin_->min_key && key <= plin_->max_key)
        {
            uint32_t ret;
            do
            {
                uint32_t i = 0;
                while (i < plin_->root_number && key >= plin_->roots[i].min_key)
                {
                    ++i;
                }
                InnerSlot *accelerator = &plin_->roots[--i];

                if (accelerator->type())
                {
                    accelerator = reinterpret_cast<InnerNode *>(accelerator->ptr)->find_leaf_node(key, accelerator);
                }
                LeafNode *leaf_to_split;
                // ret = 1 : update in a slot; ret = 2 : insert in a free slot; ret = 3 : update in overflow block; ret = 4 : insert in overflow block;
                // ret = 5 : insert in overflow block & need to split; ret = 6 : insert in overflow block & need to split orphan node; ret = 7 : the node is locked
                ret = (reinterpret_cast<LeafNode *>(accelerator->ptr))->upsert(key, payload, plin_->global_version, leaf_to_split, accelerator, false);
                // Split leaf node
                if (ret == 5)
                {
                    // std::cout << "split" << std::endl;
                    #ifdef BACKGROUND_SPLIT
                        std::thread split_thread(&SelfType::split, this, leaf_to_split, accelerator);
                        split_thread.detach();
                    #else
                        // std::cout << "split 5" << std::endl;
                        // std::cout << "key: " << key << " acclerator min key: " << accelerator->min_key << std::endl;
                        split(leaf_to_split, accelerator);
                    #endif
                }
                else if (ret == 6)
                {
#ifdef BACKGROUND_SPLIT
                    // std::cout << "background split" << std::endl;
                    std::thread split_thread(&SelfType::split, this, leaf_to_split, nullptr);
                    split_thread.detach();
#else
                    // std::cout << "split 6" << std::endl;
                    split(leaf_to_split, NULL);
#endif
                }
            } while (ret == 7);
        }

        

        // Upsert in buffer
        else if (key < plin_->min_key) {
            // Upsert in left buffer
            auto ret = plin_->left_buffer->insert({key, payload});
            if (!ret.second) { // key already exists, update the value
                ret.first->second = payload;
            }
        } else {
            // Upsert in right buffer
            auto ret = plin_->right_buffer->insert({key, payload});
            if (!ret.second) { // key already exists, update the value
                ret.first->second = payload;
            }
        }



        // else if (key < plin_->min_key)
        // {
        //     btree *left_buffer = new btree(&plin_->left_buffer, false);
        //     uint32_t ret = left_buffer->upsert(key, payload, 0);
        //     if (ret == 4)
        //     {
        //         // TODO: merge buffer
        //         if (++plin_->left_buffer_number > MAX_BUFFER)
        //         {
        //         }
        //     }
        // }
        // else
        // {
        //     btree *right_buffer = new btree(&plin_->right_buffer, false);
        //     uint32_t ret = right_buffer->upsert(key, payload, 0);
        //     if (ret == 4)
        //     {
        //         // TODO: merge buffer
        //         if (++plin_->right_buffer_number > MAX_BUFFER)
        //         {
        //         }
        //     }
        // }
    }

    void remove(_key_t key)
    {
        if (key > plin_->min_key && key < plin_->max_key)
        {
            uint32_t ret;
            do
            {
                uint32_t i = 0;
                while (i < plin_->root_number && key >= plin_->roots[i].min_key)
                {
                    ++i;
                }
                InnerSlot *accelerator = &plin_->roots[--i];
                if (accelerator->type())
                {
                    accelerator = reinterpret_cast<InnerNode *>(accelerator->ptr)->find_leaf_node(key, accelerator);
                }
                ret = reinterpret_cast<LeafNode *>(accelerator->ptr)->remove(key, plin_->global_version, accelerator);
            } while (ret == 3);
        }
        // else if (key < plin_->min_key)
        // {
        //     // btree *left_buffer = new btree(&plin_->left_buffer, false);
        //     // if (left_buffer->remove(key))
        //     // {
        //     //     --plin_->left_buffer_number;
        //     // }
        // }
        // else
        // {
        //     // btree *right_buffer = new btree(&plin_->right_buffer, false);
        //     // if (right_buffer->remove(key))
        //     // {
        //     //     --plin_->right_buffer_number;
        //     // }
        // }

        else if (key < plin_->min_key) {
        // Remove from left buffer
        size_t count = plin_->left_buffer->erase(key);
        if (count > 0) {
            --plin_->left_buffer_number;
        }
        } else  {
            // Remove from right buffer
            size_t count = plin_->right_buffer->erase(key);
            if (count > 0) {
                --plin_->right_buffer_number;
            }
        }
    }

    InnerSlot *get_parent(const InnerSlot *node)
    {
        uint32_t i = 0;
        while (i < plin_->root_number && node->min_key >= plin_->roots[i].min_key)
        {
            ++i;
        }
        --i;
        return &plin_->roots[i];
    }

    // Split & insert nodes
    void split(LeafNode *leaf_to_split, InnerSlot *accelerator = nullptr)
    {

        std::chrono::_V2::system_clock::time_point start_time = std::chrono::system_clock::now();

        LeafNode *left_sibling = leaf_to_split->get_prev();
        LeafNode *right_sibling = leaf_to_split->get_next();

        // Check wether the index is rebuilding, no smo in rebuilding process
        if (!plin_->try_get_read_lock())
        {
            return;
        }
        // Get split lock of the node, the prev node, and the next node
        if (!leaf_to_split->try_get_split_lock())
        {
            plin_->release_read_lock();
            return;
        }
        if ((!left_sibling) || (!left_sibling->try_get_split_lock()))
        {
            leaf_to_split->release_lock();
            plin_->release_read_lock();
            return;
        }
        if ((!right_sibling) || (!right_sibling->try_get_split_lock()))
        {
            if (left_sibling)
                left_sibling->release_lock();
            leaf_to_split->release_lock();
            plin_->release_read_lock();
            return;
        }

        if (accelerator)
        {
            accelerator->get_write_lock();
        }
        else
        {
            leaf_to_split->get_write_lock();
        }

        std::chrono::_V2::system_clock::time_point start_log_time = std::chrono::system_clock::now();

        std::vector<_key_t> keys;
        std::vector<_payload_t> payloads;
        // Merge & sort data in the node
        leaf_to_split->get_data(keys, payloads, plin_->global_version);

        std::vector<Segment> segments;
        auto in_fun = [keys](auto i)
        {
            return std::pair<_key_t, size_t>(keys[i], i);
        };
        auto out_fun = [&segments](auto cs)
        { segments.emplace_back(cs); };
        // Train models
        uint64_t last_n = make_segmentation(keys.size(), EPSILON_LEAF_NODE, in_fun, out_fun);
        uint64_t start_pos = 0;
        auto leaf_nodes = new LeafNode *[last_n];
        auto accelerators = new InnerSlot[last_n];
        
        LeafNode *prev = left_sibling;
        // Build leaf nodes
        for (uint64_t i = 0; i < last_n; ++i)
        {
            uint64_t block_number = (uint64_t)segments[i].number / (LEAF_NODE_INIT_RATIO * LeafNode::LeafRealSlotsPerBlock) + 3;
            uint64_t node_size_in_byte = block_number * BLOCK_SIZE + NODE_HEADER_SIZE;

            void* allocated_memory = malloc(node_size_in_byte);


            leaf_nodes[i] = new (allocated_memory) LeafNode(accelerators[i], block_number, &keys[0], &payloads[0], segments[i].number, start_pos, segments[i].slope, segments[i].intercept, plin_->global_version, prev);
            start_pos += segments[i].number;
            prev = leaf_nodes[i];
        }
        for (uint64_t i = 0; i < last_n - 1; ++i)
        {
            leaf_nodes[i]->set_next(leaf_nodes[i + 1]);
        }
        leaf_nodes[last_n - 1]->set_next(right_sibling);

        if (accelerator)
        {
            accelerator->get_read_lock();
        }
        else
        {
            leaf_to_split->get_read_lock();
            // plin_->logs[log_number].set_orphan();
        }
        // plin_->logs[log_number].left_node = leaf_nodes[0];
        // plin_->logs[log_number].right_node = leaf_nodes[last_n - 1];
        // plin_->logs[log_number].set_valid();
        //do_flush(&plin_->logs[log_number], sizeof(split_log));
        mfence();

        if (left_sibling)
            left_sibling->set_next(leaf_nodes[0]);
        if (right_sibling)
            right_sibling->set_prev(leaf_nodes[last_n - 1]);
        
        leaf_to_split->destroy();

        // Insert leaf nodes
        if (accelerator)
        {
            for (uint64_t i = 0; i < last_n; ++i)
            {
                if (leaf_nodes)
                    upsert_node(accelerators[i], leaf_nodes[i]);
                else
                    upsert_node(accelerators[i], NULL);
            }
        }
        else
        {
            plin_->orphan_number += last_n - 1;
        }
            
        delete[] accelerators;
        delete[] leaf_nodes;

        plin_->release_read_lock();
        if (left_sibling)
            left_sibling->release_lock();
        if (right_sibling)
            right_sibling->release_lock();
        // plin_->logs[log_number].release_lock();

        std::chrono::_V2::system_clock::time_point end_time = std::chrono::system_clock::now();

        // log_t += std::chrono::duration_cast<std::chrono::nanoseconds>(end_log_time - start_log_time);
        split_t += std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
        split_times++;
        // std::cout << "split done" << std::endl;

        if (double(plin_->orphan_number) / plin_->leaf_number > MAX_ORPHAN_RATIO)
        {
            std::cout << std::endl;
            std::cout << "rebuild_inner_nodes" << std::endl << std::endl;
#ifdef BACKGROUND_REBUILD
            std::thread rebuild_thread(&SelfType::rebuild_inner_nodes, this);
            rebuild_thread.detach();
#else
            rebuild_inner_nodes();
#endif
        }

    // std::cout << "split done" << std::endl;
    }

    void split_Path(LeafNode *leaf_to_split, InnerSlot *accelerator, std::vector<InnerSlot *> *last_slots = nullptr)
    {

        std::chrono::_V2::system_clock::time_point start_time = std::chrono::system_clock::now();

        LeafNode *left_sibling = leaf_to_split->get_prev();
        LeafNode *right_sibling = leaf_to_split->get_next();

        // Check wether the index is rebuilding, no smo in rebuilding process
        if (!plin_->try_get_read_lock())
        {
            return;
        }
        // Get split lock of the node, the prev node, and the next node
        if (!leaf_to_split->try_get_split_lock())
        {
            plin_->release_read_lock();
            return;
        }
        if ((!left_sibling) || (!left_sibling->try_get_split_lock()))
        {
            leaf_to_split->release_lock();
            plin_->release_read_lock();
            return;
        }
        if ((!right_sibling) || (!right_sibling->try_get_split_lock()))
        {
            if (left_sibling)
                left_sibling->release_lock();
            leaf_to_split->release_lock();
            plin_->release_read_lock();
            return;
        }

        if (accelerator)
        {
            accelerator->get_write_lock();
        }
        else
        {
            leaf_to_split->get_write_lock();
        }

        std::chrono::_V2::system_clock::time_point start_log_time = std::chrono::system_clock::now();

        std::vector<_key_t> keys;
        std::vector<_payload_t> payloads;
        // Merge & sort data in the node
        leaf_to_split->get_data(keys, payloads, plin_->global_version);

        std::vector<Segment> segments;
        auto in_fun = [keys](auto i)
        {
            return std::pair<_key_t, size_t>(keys[i], i);
        };
        auto out_fun = [&segments](auto cs)
        { segments.emplace_back(cs); };
        // Train models
        uint64_t last_n = make_segmentation(keys.size(), EPSILON_LEAF_NODE, in_fun, out_fun);
        uint64_t start_pos = 0;
        auto leaf_nodes = new LeafNode *[last_n];
        auto accelerators = new InnerSlot[last_n];

        // std::cout << "leaf number: " << last_n << std::endl;
        
        LeafNode *prev = left_sibling;
        // Build leaf nodes
        for (uint64_t i = 0; i < last_n; ++i)
        {
            uint64_t block_number = (uint64_t)segments[i].number / (LEAF_NODE_INIT_RATIO * LeafNode::LeafRealSlotsPerBlock) + 3;
            uint64_t node_size_in_byte = block_number * BLOCK_SIZE + NODE_HEADER_SIZE;

            void* allocated_memory = malloc(node_size_in_byte);

            if (i == 0)
                leaf_nodes[i] = new (allocated_memory) LeafNode(accelerators[i], block_number, &keys[0], &payloads[0], segments[i].number, start_pos, segments[i].slope, segments[i].intercept, plin_->global_version, prev, nullptr, accelerator->leaf_number);
            else
                leaf_nodes[i] = new (allocated_memory) LeafNode(accelerators[i], block_number, &keys[0], &payloads[0], segments[i].number, start_pos, segments[i].slope, segments[i].intercept, plin_->global_version, prev, nullptr, 10000);
            start_pos += segments[i].number;
            prev = leaf_nodes[i];
        }
        for (uint64_t i = 0; i < last_n - 1; ++i)
        {
            leaf_nodes[i]->set_next(leaf_nodes[i + 1]);
        }
        leaf_nodes[last_n - 1]->set_next(right_sibling);
        // std::cout << "last split.next : " << right_sibling->get_Slot()->leaf_number << std::endl;

        if (accelerator)
        {
            accelerator->get_read_lock();
        }
        else
        {
            leaf_to_split->get_read_lock();
            // plin_->logs[log_number].set_orphan();
        }
        // plin_->logs[log_number].left_node = leaf_nodes[0];
        // plin_->logs[log_number].right_node = leaf_nodes[last_n - 1];
        // plin_->logs[log_number].set_valid();
        //do_flush(&plin_->logs[log_number], sizeof(split_log));
        mfence();

        if (left_sibling)
            left_sibling->set_next(leaf_nodes[0]);
        if (right_sibling)
            right_sibling->set_prev(leaf_nodes[last_n - 1]);
        
        leaf_to_split->destroy();

        // Insert leaf nodes
        if (accelerator)
        {
            for (uint64_t i = 0; i < last_n; ++i)
            {
                upsert_node(accelerators[i], leaf_nodes[i], last_slots);
            }
        }
        else
        {
            plin_->orphan_number += last_n - 1;
        }

        // if (last_slots) {
        //     std::cout << std::endl;
        //     std::cout << "original last slot status: " << std::endl;
        //     InnerSlot* leaf = plin_->last_slots->at(0);
        //     while(leaf) {
        //         std::cout << "leaf number: " << leaf->leaf_number << std::endl;
        //         if (reinterpret_cast<LeafNode *>(leaf->ptr)->get_next())
        //             leaf = reinterpret_cast<LeafNode *>(leaf->ptr)->get_next()->get_Slot();
        //         else 
        //             break;
        //     }
        //     // for(InnerSlot* slot : *(plin_->last_slots)) {
        //     //     std::cout << "leaf number: " << slot->leaf_number << std::endl;
        //     // }
        //     std::cout << std::endl;
        // }
            
            // if (last_slots && leaf_to_split->get_Slot()->leaf_number != 10000) {
            //     plin_->last_slots->at(leaf_to_split->get_Slot()->leaf_number) = leaf_nodes[0]->get_Slot();
            // std::cout << "leaf_to_split: " << leaf_to_split->get_Slot()->leaf_number << std::endl;
            //     // std::cout << "original leaf number: " << leaf_to_split->get_Slot()->leaf_number << std::endl;
            //     // std::cout << "plin leaf number: " << plin_->last_slots->at(leaf_to_split->get_Slot()->leaf_number)->leaf_number << std::endl;
            // }
               

        // if (last_slots) {
        //     std::cout << std::endl;
        //     std::cout << "current last slot status: " << std::endl;
        //     InnerSlot* leaf = plin_->last_slots->at(0);
        //     while(leaf) {
        //         std::cout << "leaf number: " << leaf->leaf_number << std::endl;
        //         if (reinterpret_cast<LeafNode *>(leaf->ptr)->get_next())
        //             leaf = reinterpret_cast<LeafNode *>(leaf->ptr)->get_next()->get_Slot();
        //         else 
        //             break;
        //     }
        //     // for(InnerSlot* slot : *(plin_->last_slots)) {
        //     //     std::cout << "leaf number: " << slot->leaf_number << std::endl;

        //     // }
        //     std::cout << std::endl;
        // }
            
        delete[] accelerators;
        delete[] leaf_nodes;

        plin_->release_read_lock();
        if (left_sibling)
            left_sibling->release_lock();
        if (right_sibling)
            right_sibling->release_lock();
        // plin_->logs[log_number].release_lock();

        std::chrono::_V2::system_clock::time_point end_time = std::chrono::system_clock::now();

        // log_t += std::chrono::duration_cast<std::chrono::nanoseconds>(end_log_time - start_log_time);
        split_t += std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
        split_times++;
        // std::cout << "split done" << std::endl;

        if (double(plin_->orphan_number) / plin_->leaf_number > MAX_ORPHAN_RATIO)
        {
            std::cout << std::endl;
            std::cout << "rebuild_inner_nodes" << std::endl << std::endl;
#ifdef BACKGROUND_REBUILD
            std::thread rebuild_thread(&SelfType::rebuild_inner_nodes, this);
            rebuild_thread.detach();
#else
            rebuild_inner_nodes();
#endif
        }

    // std::cout << "split done" << std::endl;
    }

    void upsert_node(InnerSlot &node, LeafNode* leaf_node = nullptr, std::vector<InnerSlot *> *last_slots = nullptr)
    {
        uint32_t i = 0;
        while (i < plin_->root_number && node.min_key >= plin_->roots[i].min_key)
        {
            ++i;
        }
        --i;
        uint32_t ret;

        ret = reinterpret_cast<InnerNode *>(plin_->roots[i].ptr)->upsert_node(node, &plin_->roots[i], leaf_node, last_slots);

        if (ret > 1)
        {
            ++plin_->leaf_number;
        }
        if (ret > 2)
        {
            ++plin_->orphan_number;
        }
        // std::cout << "upsert node" << std::endl;
    }

    LeafNode *get_leftmost_leaf()
    {
        return reinterpret_cast<InnerNode *>(plin_->roots[0].ptr)->get_leftmost_leaf();
    }

    // Rebuild inner nodes, allow insert and search, no SMO
    void rebuild_inner_nodes()
    {


        std::chrono::_V2::system_clock::time_point start_time = std::chrono::system_clock::now();

        // Get leaf nodes
        if (!plin_->get_write_lock())
            return;

        uint64_t last_n = plin_->leaf_number;
        auto first_keys = new _key_t[last_n];
        auto accelerators = new InnerSlot[last_n];
        auto leaf_nodes = new LeafNode*[last_n];
        LeafNode *node = get_leftmost_leaf();

        for (uint64_t i = 0; i < last_n; ++i)
        {
            leaf_nodes[i] = node;
            node->get_info(first_keys[i], accelerators[i]);
            node = node->get_next();
        }

        // Build inner nodes recursively
        std::vector<Segment> segments;
        uint32_t level = 0;
        uint64_t offset = 0;
        std::vector<InnerSlot*> *last_slots = new std::vector<InnerSlot*>();;
        auto accelerators_tmp = accelerators;
        auto first_keys_tmp = first_keys;
        auto in_fun_rec = [&first_keys](auto i)
        {
            return std::pair<_key_t, size_t>(first_keys[i], i);
        };
        auto out_fun = [&segments](auto cs)
        { segments.emplace_back(cs); };
        while (level == 0 || last_n > ROOT_SIZE)
        {
            level++;
            last_n = make_segmentation(last_n, EPSILON_INNER_NODE, in_fun_rec, out_fun);
            std::cout<<"Number of inner nodes: "<<last_n<<std::endl;
            uint64_t start_pos = 0;
            first_keys = new _key_t[last_n];
            accelerators = new InnerSlot[last_n];
            for (uint64_t i = 0; i < last_n; ++i)
            {
                uint64_t block_number = (uint64_t)segments[offset + i].number / (INNER_NODE_INIT_RATIO * InnerNode::InnerSlotsPerBlock) + EPSILON_INNER_NODE / InnerNode::InnerSlotsPerBlock + 2;
                uint64_t node_size_in_byte = block_number * BLOCK_SIZE + NODE_HEADER_SIZE;
                first_keys[i] = segments[offset + i].first_key;

                void* allocated_memory = malloc(node_size_in_byte);

                if (level == 1)
                    new (allocated_memory) InnerNode(accelerators[i], leaf_nodes, block_number, first_keys_tmp, accelerators_tmp, segments[offset + i].number, start_pos, segments[offset + i].slope, segments[offset + i].intercept, level, plin_->last_slots);
                else
                    new (allocated_memory) InnerNode(accelerators[i], nullptr, block_number, first_keys_tmp, accelerators_tmp, segments[offset + i].number, start_pos, segments[offset + i].slope, segments[offset + i].intercept, level);
                start_pos += segments[offset + i].number;
            }
            delete[] accelerators_tmp;
            delete[] first_keys_tmp;
            delete[] leaf_nodes;
            accelerators_tmp = accelerators;
            first_keys_tmp = first_keys;
            offset += last_n;
        }
        plin_->rebuilding = 1;
        plin_metadata *old_plin_ = new plin_metadata();
        plin_->old_plin_ = old_plin_;
        old_plin_->root_number = plin_->root_number;
        for (uint32_t i = 0; i < plin_->root_number; ++i)
            old_plin_->roots[i] = plin_->roots[i];
        old_plin_->level = plin_->level;
        old_plin_->orphan_number = plin_->orphan_number;
        //do_flush(old_plin_, sizeof(plin_metadata));
        mfence();

        // Build new root
        plin_->rebuilding = 2;
        plin_->root_number = last_n;
        plin_->level = level;
        plin_->orphan_number = 0;
        plin_->last_slots = last_slots;
        for (uint32_t i = 0; i < last_n; ++i)
        {
            plin_->roots[i] = accelerators_tmp[i];
        }
        for (uint32_t i = last_n; i < ROOT_SIZE; ++i)
        {
            plin_->roots[i].min_key = FREE_FLAG;
        }
        plin_->release_write_lock();
        for (uint32_t i = 0; i < old_plin_->root_number; i++)
        {
            ((InnerNode *)(old_plin_->roots[i].ptr))->destroy();
        }
        plin_->rebuilding = 0;
        delete old_plin_;
        plin_->old_plin_ = NULL;
       // do_flush(plin_, sizeof(plin_metadata));
        mfence();
        delete[] accelerators_tmp;
        delete[] first_keys_tmp;

        std::chrono::_V2::system_clock::time_point end_time = std::chrono::system_clock::now();

        rebuild_t += std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
        rebuild_times++;

        std::cout << "rebuild done" << std::endl;

    }
};

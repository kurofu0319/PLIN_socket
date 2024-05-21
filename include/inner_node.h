//
// Copyright (c) Zhou Zhang.
// Implementation of PLIN inner nodes
//

#pragma once

#include "leaf_node.h"
#include <functional>
#include <cstring>

class InnerNode{

#pragma pack(push)
#pragma pack(1)

    // The size of header should less than NODE_HEADER_SIZE (256B)
    struct InnerNodeMetadata {
        double slope = 0;
        float intercept = 0;
        uint32_t block_number = 0;
        uint64_t record_number = 0;
        _key_t first_key = FREE_FLAG;
        uint32_t level = 0;
    } inner_node;
    char unused[NODE_HEADER_SIZE - sizeof(InnerNodeMetadata)];

#pragma pack(pop)

    InnerSlot inner_slots[1]; 

public:

    constexpr static uint64_t InnerSlotsPerBlock = BLOCK_SIZE / (sizeof(InnerSlot));

    inline uint32_t predict_block (_key_t key, double slope, float intercept, uint32_t block_number) const {
        int64_t predicted_block = (key * slope + intercept + 0.5) / InnerSlotsPerBlock;
        if (predicted_block < 0)
            return 0;
        else if (predicted_block > block_number - 3)
            return block_number - 2;
        return predicted_block + 1;
    }

    InnerNode(InnerSlot& accelerator, uint64_t block_number, _key_t* keys, InnerSlot* nodes, uint64_t number, uint64_t start_pos, double slope, double intercept, uint32_t level, bool rebuild = false) {
        // assert((uint64_t)inner_slots - (uint64_t)&inner_node == NODE_HEADER_SIZE);
        inner_node.block_number = block_number;
        inner_node.slope = slope / INNER_NODE_INIT_RATIO;
        inner_node.intercept = (intercept - start_pos - keys[start_pos] * slope) / INNER_NODE_INIT_RATIO;
        inner_node.record_number = number;
        inner_node.first_key = keys[start_pos];
        inner_node.level = level;
        //do_flush(&inner_node, sizeof(inner_node));
        model_correction(inner_node.slope, inner_node.intercept, (inner_node.block_number - 3) * InnerSlotsPerBlock, keys[start_pos], keys[start_pos + number - 1]);
        // Init
        for (uint64_t i = 0; i < inner_node.block_number; ++i) {
            for (uint8_t j = 0; j < 8; j++) {
                inner_slots[i * InnerSlotsPerBlock + j].min_key = FREE_FLAG;
            }
        }
        // Model-based data placement
        for (uint64_t i = 0; i < number; ++i) {
            data_placement(keys[start_pos + i], nodes[start_pos + i]);
        }
        // do_flush(inner_slots, block_number * BLOCK_SIZE);
        // Build accelerator
        accelerator.min_key = inner_node.first_key;
        accelerator.ptr = this;
        accelerator.slope = inner_node.slope;
        accelerator.intercept = inner_node.intercept;
        accelerator.set_block_number(inner_node.block_number);
        accelerator.set_type(1);
        accelerator.init_lock();
    }
    
    ~InnerNode() {}

    void destroy () {
        if (inner_node.level > 1) {
            uint64_t slot_number = inner_node.block_number * InnerSlotsPerBlock;
            for (uint64_t slot = 0; slot < slot_number; ++slot) {
                if (inner_slots[slot].min_key != FREE_FLAG) {
                    reinterpret_cast<InnerNode*>(inner_slots[slot].ptr)->destroy();
                }
            }
        }
        free(this);
    }

    void traverseAllInnerSlots(std::function<void(const InnerSlot&)> action) {
        std::cout << "level: " << inner_node.level << std::endl;
        for (uint64_t slot = 0; slot < inner_node.block_number * InnerSlotsPerBlock; ++slot) {
            if (inner_slots[slot].min_key != FREE_FLAG) {
                action(inner_slots[slot]);  // 应用传入的函数操作
                
                // 如果当前slot指向的是另一个InnerNode，则递归遍历
                if (inner_slots[slot].type()) {  
                    std::cout << "traverse recursively" << std::endl;
                    (reinterpret_cast<InnerNode*>(inner_slots[slot].ptr))->traverseAllInnerSlots(action);
                }
            }
        }
    }

    void serializeInnerNode(std::vector<char>& buffer) 
    {
        // 序列化当前节点
        size_t metadata_size = sizeof(InnerNode::InnerNodeMetadata);
        
        const char* metadata_ptr = reinterpret_cast<const char*>(&inner_node);
        buffer.insert(buffer.end(), metadata_ptr, metadata_ptr + metadata_size);

        // 序列化slots
        size_t slots_size = inner_node.block_number * InnerNode::InnerSlotsPerBlock * sizeof(InnerSlot);

        const char* slots_ptr = reinterpret_cast<const char*>(inner_slots);
        buffer.insert(buffer.end(), slots_ptr, slots_ptr + slots_size);

        // 递归序列化子节点
        for (uint64_t i = 0; i < inner_node.block_number * InnerNode::InnerSlotsPerBlock; ++i) {
            if (inner_slots[i].type()) {  // 如果是内部节点
                std::cout << "serializeInnerNode recursively " << std::endl;
                (reinterpret_cast<InnerNode*>(inner_slots[i].ptr))->serializeInnerNode(buffer);
            }
        }
    }

    void deserializeInnerNode(const std::vector<char>& buffer, size_t& offset) {
        // 确保有足够的数据读取元数据
        if (offset + sizeof(InnerNodeMetadata) > buffer.size()) {
            std::cerr << "Buffer overflow when trying to read metadata." << std::endl;
            return;
        }

        // 反序列化元数据
        memcpy(&inner_node, buffer.data() + offset, sizeof(InnerNodeMetadata));
        offset += sizeof(inner_node);

        size_t slots_size = inner_node.block_number * InnerSlotsPerBlock * sizeof(InnerSlot);

        // 检查是否有足够的数据来反序列化所有 slots
        if (offset + slots_size > buffer.size()) {
            std::cerr << "Buffer overflow when trying to read slots." << std::endl;
            return;
        }

        // 反序列化 slots 数据
        memcpy(inner_slots, buffer.data() + offset, slots_size);

        // for (int i = 0; i < metadata.block_number * InnerSlotsPerBlock; i ++ )
        //     inner_slots[i] = slots[i];

        // std::cout << slots_size << std::endl;
        

        // 反序列化每个 slot 的指针，如果是 InnerNode 类型
        for (uint64_t i = 0; i < inner_node.block_number * InnerSlotsPerBlock; ++i) {
            if (inner_slots[i].type()) {
                // TODO:
                if (inner_slots[i].ptr != nullptr) {
                    reinterpret_cast<InnerNode*>(inner_slots[i].ptr)->deserializeInnerNode(buffer, offset);
                }
            }
        }
    }

    InnerSlot * get_Slot (uint64_t slot) {
        return &inner_slots[slot];
    }

    InnerSlot * find_leaf_node (_key_t key, const InnerSlot * accelerator) {
        uint32_t block_number = accelerator->block_number();
        uint64_t slot = predict_block(key, accelerator->slope, accelerator->intercept, block_number) * InnerSlotsPerBlock;
        
        // Search left

        if (inner_slots[slot].min_key == FREE_FLAG || key < inner_slots[slot].min_key) {
            while (inner_slots[--slot].min_key == FREE_FLAG) {}
        }
        // Search right
        else {
            uint32_t slot_number = block_number * InnerSlotsPerBlock;
            while (slot < slot_number && inner_slots[slot].min_key != FREE_FLAG && key >= inner_slots[slot].min_key) {
                ++slot;
            }
            --slot;
        }

        // std::cout << "find slot: " << slot << std::endl;

        // if (key - 35497497.707480 < 0.1 && key - 35497497.707480 > -0.1)
        // {
        //     std::cout << "KEY is 35497497.707480" << std::endl;
        //     std::cout << slot << std::endl;
        //     std::cout << inner_slots[slot].min_key << std::endl;
        // }
            

        if (inner_slots[slot].type()) {
            return reinterpret_cast<InnerNode*>(inner_slots[slot].ptr)->find_leaf_node(key, &inner_slots[slot]);
        }
        else {
            return &inner_slots[slot];
        }
    }

    void get_Leaf_path (_key_t key, const InnerSlot * accelerator, std::vector<int> &leaf_path)
    {
        uint32_t block_number = accelerator->block_number();
        uint64_t slot = predict_block(key, accelerator->slope, accelerator->intercept, block_number) * InnerSlotsPerBlock;

        // std::cout << slot << std::endl;

        
        // Search left
        if (inner_slots[slot].min_key == FREE_FLAG || key < inner_slots[slot].min_key) {
            while (inner_slots[--slot].min_key == FREE_FLAG) {}
        }
        // Search right
        else {
            uint32_t slot_number = block_number * InnerSlotsPerBlock;
            while (slot < slot_number && inner_slots[slot].min_key != FREE_FLAG && key >= inner_slots[slot].min_key) {
                ++slot;
            }
            --slot;
        }

        

        leaf_path.push_back(slot);

        if (inner_slots[slot].type()) {
            reinterpret_cast<InnerNode*>(inner_slots[slot].ptr)->get_Leaf_path(key, &inner_slots[slot], leaf_path);
        }

    }

    // Upsert new nodes
    uint32_t upsert_node (InnerSlot& node, InnerSlot * accelerator) {
        uint32_t block_number = accelerator->block_number();
        uint64_t slot = predict_block(node.min_key, accelerator->slope, accelerator->intercept, block_number) * InnerSlotsPerBlock;
        uint64_t predicted_slot = slot;
        // Search left
        if (inner_slots[slot].min_key == FREE_FLAG || node.min_key < inner_slots[slot].min_key) {
            while (inner_slots[--slot].min_key == FREE_FLAG) {}
        }
        // Search right
        else {
            uint32_t slot_number = block_number * InnerSlotsPerBlock;
            while (slot < slot_number && inner_slots[slot].min_key != FREE_FLAG && node.min_key >= inner_slots[slot].min_key) {
                ++slot;
            }
            --slot;
        }
        if (inner_slots[slot].type()) {
            return reinterpret_cast<InnerNode*>(inner_slots[slot].ptr)->upsert_node(node, &inner_slots[slot]);
        }
        // Upsert node
        else {
            if (inner_slots[slot].min_key == node.min_key) {
                accelerator->get_lock();
                inner_slots[slot] = node;
                //do_flush(&inner_slots[slot], sizeof(InnerSlot));
                mfence();
                accelerator->release_lock();
                return 1;
            }
            else if (inner_slots[predicted_slot + InnerSlotsPerBlock - 1].min_key == FREE_FLAG) {
                accelerator->get_lock();
                uint64_t target_slot = predicted_slot;
                if (slot >= predicted_slot) {
                    while (inner_slots[++target_slot].min_key <= node.min_key) {}
                }
                slot = predicted_slot + InnerSlotsPerBlock - 1;
                while (inner_slots[--slot].min_key == FREE_FLAG) {}
                for (++slot; slot > target_slot; --slot) {
                    inner_slots[slot] = inner_slots[slot - 1];
                }
                inner_slots[target_slot] = node;
                //do_flush(&inner_slots[predicted_slot], InnerSlotsPerBlock * sizeof(InnerSlot));
                mfence();
                accelerator->release_lock();
                return 2;
            }
            else {
                return 3;
            }
        }
    }

    void data_placement (_key_t key, InnerSlot node) {
        uint64_t slot = predict_block(key, inner_node.slope, inner_node.intercept, inner_node.block_number) * InnerSlotsPerBlock;
        for (uint32_t i = 0; i < InnerSlotsPerBlock; ++i) {
            if (inner_slots[slot + i].min_key == FREE_FLAG) {                
                inner_slots[slot + i] = node;
                return;
            }
        }
        for (slot += InnerSlotsPerBlock; slot < inner_node.block_number * InnerSlotsPerBlock; slot++) {
            if (inner_slots[slot].min_key == FREE_FLAG) {                
                inner_slots[slot] = node;
                return;
            }
        }
        assert(slot < inner_node.block_number * InnerSlotsPerBlock);
        return;
    }

    LeafNode* get_leftmost_leaf(){
        uint64_t slot = 0;
        while(inner_slots[slot].min_key == FREE_FLAG) {
            ++slot;
        }
        if (inner_slots[slot].type() == 0) {
            return reinterpret_cast<LeafNode*>(inner_slots[slot].ptr);
        }
        else {
            return reinterpret_cast<InnerNode*>(inner_slots[slot].ptr)->get_leftmost_leaf();
        }
    }

};

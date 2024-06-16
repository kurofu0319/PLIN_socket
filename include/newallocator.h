/*  
    A wrapper for using PMDK allocator easily (refer to pmwcas)
    Copyright (c) Luo Yongping. THIS SOFTWARE COMES WITH NO WARRANTIES, 
    USE AT YOUR OWN RISK!
*/

#pragma once

static const int PIECE_CNT = 64;
static const size_t ALIGN_SIZE = 256;
    
void * new_malloc(size_t nsize) { 

       // std::cout << "new malloc" << std::endl;

        void * mem = malloc(nsize + ALIGN_SIZE); // not aligned
        //  |  UNUSED    |HEADER|       memory you can use     |
        // mem             (mem + off)
        uint64_t offset = ALIGN_SIZE - (uint64_t)mem % ALIGN_SIZE;
        // store a header in the front
        uint64_t * header = (uint64_t *)((uint64_t)mem + offset - 8);
        *header = offset;

        return (void *)((uint64_t)mem + offset);

}




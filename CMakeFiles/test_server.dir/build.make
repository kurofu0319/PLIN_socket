# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.29

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /snap/cmake/1399/bin/cmake

# The command to remove a file.
RM = /snap/cmake/1399/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/furui/Desktop/PLIN_socket

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/furui/Desktop/PLIN_socket

# Include any dependencies generated for this target.
include CMakeFiles/test_server.dir/depend.make
# Include any dependencies generated by the compiler for this target.
include CMakeFiles/test_server.dir/compiler_depend.make

# Include the progress variables for this target.
include CMakeFiles/test_server.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/test_server.dir/flags.make

CMakeFiles/test_server.dir/test_server.cpp.o: CMakeFiles/test_server.dir/flags.make
CMakeFiles/test_server.dir/test_server.cpp.o: test_server.cpp
CMakeFiles/test_server.dir/test_server.cpp.o: CMakeFiles/test_server.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green --progress-dir=/home/furui/Desktop/PLIN_socket/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/test_server.dir/test_server.cpp.o"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT CMakeFiles/test_server.dir/test_server.cpp.o -MF CMakeFiles/test_server.dir/test_server.cpp.o.d -o CMakeFiles/test_server.dir/test_server.cpp.o -c /home/furui/Desktop/PLIN_socket/test_server.cpp

CMakeFiles/test_server.dir/test_server.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green "Preprocessing CXX source to CMakeFiles/test_server.dir/test_server.cpp.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/furui/Desktop/PLIN_socket/test_server.cpp > CMakeFiles/test_server.dir/test_server.cpp.i

CMakeFiles/test_server.dir/test_server.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green "Compiling CXX source to assembly CMakeFiles/test_server.dir/test_server.cpp.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/furui/Desktop/PLIN_socket/test_server.cpp -o CMakeFiles/test_server.dir/test_server.cpp.s

# Object files for target test_server
test_server_OBJECTS = \
"CMakeFiles/test_server.dir/test_server.cpp.o"

# External object files for target test_server
test_server_EXTERNAL_OBJECTS =

test_server: CMakeFiles/test_server.dir/test_server.cpp.o
test_server: CMakeFiles/test_server.dir/build.make
test_server: /usr/lib/x86_64-linux-gnu/libgflags.so.2.2.2
test_server: CMakeFiles/test_server.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green --bold --progress-dir=/home/furui/Desktop/PLIN_socket/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable test_server"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/test_server.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/test_server.dir/build: test_server
.PHONY : CMakeFiles/test_server.dir/build

CMakeFiles/test_server.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/test_server.dir/cmake_clean.cmake
.PHONY : CMakeFiles/test_server.dir/clean

CMakeFiles/test_server.dir/depend:
	cd /home/furui/Desktop/PLIN_socket && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/furui/Desktop/PLIN_socket /home/furui/Desktop/PLIN_socket /home/furui/Desktop/PLIN_socket /home/furui/Desktop/PLIN_socket /home/furui/Desktop/PLIN_socket/CMakeFiles/test_server.dir/DependInfo.cmake "--color=$(COLOR)"
.PHONY : CMakeFiles/test_server.dir/depend


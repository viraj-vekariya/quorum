CXX := c++
CXXFLAGS := -std=c++17 -Wall -Wextra -g -Iinclude -pthread
TSAN_FLAGS := -fsanitize=thread

SRC := src/transport.cpp src/raft_node.cpp
BUILD := build

.PHONY: all demo test tsan clean

all: demo test

demo: $(BUILD)/quorum_demo

test: $(BUILD)/quorum_test
	$(BUILD)/quorum_test

tsan: $(BUILD)/quorum_test_tsan
	$(BUILD)/quorum_test_tsan

$(BUILD)/quorum_demo: $(SRC) src/main.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) $(SRC) src/main.cpp -o $@

$(BUILD)/quorum_test: $(SRC) tests/test_raft.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) $(SRC) tests/test_raft.cpp -o $@

$(BUILD)/quorum_test_tsan: $(SRC) tests/test_raft.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) $(TSAN_FLAGS) $(SRC) tests/test_raft.cpp -o $@

$(BUILD):
	mkdir -p $(BUILD)

clean:
	rm -rf $(BUILD)

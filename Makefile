CXX := g++
CXXFLAGS := -std=c++17 -O3 -Wall -Wextra -MMD -MP
LDFLAGS := -pthread

TARGET := titan-event-runtime
BUILD_DIR := build
SRC_DIR := src
TEST_DIR := tests
BENCH_DIR := benchmarks

SRCS := $(wildcard $(SRC_DIR)/*.cpp)
OBJS := $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(SRCS))
DEPS := $(OBJS:.o=.d)

TEST_SRCS := $(wildcard $(TEST_DIR)/*.cpp)
TEST_OBJS := $(patsubst $(TEST_DIR)/%.cpp, $(BUILD_DIR)/test_%.o, $(TEST_SRCS))
TEST_TARGET := $(BUILD_DIR)/run_tests

BENCH_SRCS := $(wildcard $(BENCH_DIR)/*.cpp)
BENCH_OBJS := $(patsubst $(BENCH_DIR)/%.cpp, $(BUILD_DIR)/bench_%.o, $(BENCH_SRCS))
BENCH_TARGET := $(BUILD_DIR)/run_benchmarks

.PHONY: all build test benchmark clean

all: build

$(BUILD_DIR):
	@mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

build: $(BUILD_DIR)/$(TARGET)

$(BUILD_DIR)/$(TARGET): $(OBJS)
	$(CXX) $(OBJS) -o $@ $(LDFLAGS)

$(BUILD_DIR)/test_%.o: $(TEST_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(TEST_TARGET): $(OBJS) $(TEST_OBJS)
	$(CXX) $(filter-out $(BUILD_DIR)/main.o, $(OBJS)) $(TEST_OBJS) -o $@ $(LDFLAGS)

test: $(BUILD_DIR)
	@if [ -d "$(TEST_DIR)" ] && [ -n "$(TEST_SRCS)" ]; then \
		$(MAKE) $(TEST_TARGET) && $(TEST_TARGET); \
	else \
		echo "No tests found in $(TEST_DIR). Skipping."; \
	fi

$(BUILD_DIR)/bench_%.o: $(BENCH_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BENCH_TARGET): $(OBJS) $(BENCH_OBJS)
	$(CXX) $(filter-out $(BUILD_DIR)/main.o, $(OBJS)) $(BENCH_OBJS) -o $@ $(LDFLAGS)

benchmark: $(BUILD_DIR)
	@if [ -d "$(BENCH_DIR)" ] && [ -n "$(BENCH_SRCS)" ]; then \
		$(MAKE) $(BENCH_TARGET) && $(BENCH_TARGET); \
	else \
		echo "No benchmarks found in $(BENCH_DIR). Skipping."; \
	fi

clean:
	@rm -rf $(BUILD_DIR)

-include $(DEPS)
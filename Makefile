SOURCES=$(wildcard *.cpp)
OBJS=$(SOURCES:%.cpp=%.o)
TARGETS=$(SOURCES:%.cpp=%)

CPPFLAGS += -g -pthread -std=c++11 -Wall 
LDFLAGS += -pthread
LDLIBS += -lrt

.PHONY: all clean 

all: $(TARGETS)

%.o: %.cpp
	g++ -c $(CPPFLAGS) $^ -o $@
	
$(TARGETS): %: %.o
	g++ $(LDFLAGS) $^ $(LDLIBS) -o $@ 

clean:
	rm -rf *.o $(TARGETS)
	
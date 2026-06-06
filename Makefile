CXX      := g++
CXXFLAGS := -O3 -march=native -std=c++17 -Wall -Wextra -pthread
LDFLAGS  := -lpthread
TARGET   := pixelflut
SRC      := pixelflut.cpp
STB_URL  := https://raw.githubusercontent.com/nothings/stb/master/stb_image.h

.PHONY: all clean

all: stb_image.h $(TARGET)

$(TARGET): $(SRC) stb_image.h
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
	@echo "Build OK → ./$(TARGET)"

stb_image.h:
	@echo "Downloading stb_image.h..."
	curl -sSL $(STB_URL) -o stb_image.h

clean:
	rm -f $(TARGET)

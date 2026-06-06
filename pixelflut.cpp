// ============================================================================
// pixelflut.cpp — High-performance Pixelflut client
// ============================================================================
//
// Compile:
//   g++ -O3 -march=native -std=c++17 -o pixelflut pixelflut.cpp -lpthread
//
// Usage:
//   ./pixelflut <host> <port> <image.png> <center_x> <center_y> [threads=8]
//
// What it does:
//   1. Loads image.png (any format with alpha supported by stb_image).
//   2. Places its centre at (center_x, center_y) on the Pixelflut canvas.
//   3. Queries canvas SIZE and clips out-of-bounds pixels.
//   4. Skips fully-transparent pixels (alpha == 0).
//   5. Builds DISTINCT_BUFS independently-shuffled command buffers in memory.
//   6. Spawns <threads> TCP connections that blast their buffer in a tight loop.
//
// Performance design:
//   • Commands are pre-formatted with hand-written integer/hex writers
//     (10–15× faster than sprintf).
//   • Each thread keeps its connection alive and reconnects automatically.
//   • TCP_NODELAY + large SO_SNDBUF maximise kernel throughput.
//   • Multiple independently-shuffled buffers ensure uniform canvas coverage.
//   • A live stats line reports MB/s every second.
// ============================================================================

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <csignal>
#include <cerrno>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <atomic>
#include <vector>
#include <algorithm>
#include <random>

// stb_image: single-header image loader (PNG, JPEG, BMP, …)
#define STB_IMAGE_IMPLEMENTATION
#define STBI_ONLY_PNG
#define STBI_FAILURE_USERMSG
#include "stb_image.h"

// ─── Tunables ────────────────────────────────────────────────────────────────

static constexpr int DEFAULT_THREADS  = 8;
static constexpr int MAX_THREADS      = 512;
// How many distinct shuffled command buffers to build.
// Threads round-robin among them, so neighbours send different pixel orders.
static constexpr int DISTINCT_BUFS    = 4;
// Kernel send-buffer size per socket (bytes).
static constexpr int SOCK_SNDBUF_SZ   = 1 << 23; // 8 MiB

// ─── Globals ─────────────────────────────────────────────────────────────────

static volatile sig_atomic_t g_run   = 1;
static std::atomic<uint64_t> g_bytes { 0 };

// ─── Per-thread argument ──────────────────────────────────────────────────────

struct WorkerArg {
    const char *host;
    int         port;
    const char *buf;   // read-only command buffer
    size_t      len;
};

// ─── Signal handler ──────────────────────────────────────────────────────────

static void on_signal(int) { g_run = 0; }

// ─────────────────────────────────────────────────────────────────────────────
//  Fast command formatters — avoid all printf overhead
// ─────────────────────────────────────────────────────────────────────────────

static const char HEX_UPPER[] = "0123456789ABCDEF";

// Write a base-10 unsigned integer (no leading zeros, handles 0 correctly).
static inline char *write_uint(char *p, unsigned v)
{
    char tmp[6]; int n = 0;
    do { tmp[n++] = char('0' + v % 10); v /= 10; } while (v);
    while (n > 0) *p++ = tmp[--n];
    return p;
}

// Write exactly two uppercase hex digits.
static inline char *write_hex8(char *p, uint8_t v)
{
    *p++ = HEX_UPPER[v >> 4];
    *p++ = HEX_UPPER[v & 0xF];
    return p;
}

// Write a full "PX x y RRGGBB\n" or "PX x y RRGGBBAA\n" command.
// Omits the alpha byte when alpha == 255 to save bandwidth.
static inline char *write_px_cmd(char  *p,
                                  uint16_t x, uint16_t y,
                                  uint8_t  r, uint8_t  g,
                                  uint8_t  b, uint8_t  a)
{
    *p++ = 'P'; *p++ = 'X'; *p++ = ' ';
    p = write_uint(p, x);  *p++ = ' ';
    p = write_uint(p, y);  *p++ = ' ';
    p = write_hex8(p, r);
    p = write_hex8(p, g);
    p = write_hex8(p, b);
    if (a != 255) p = write_hex8(p, a);
    *p++ = '\n';
    return p;
}

// ─────────────────────────────────────────────────────────────────────────────
//  TCP helpers
// ─────────────────────────────────────────────────────────────────────────────

static int tcp_connect(const char *host, int port)
{
    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags    = AI_ADDRCONFIG;

    char port_s[8];
    snprintf(port_s, sizeof(port_s), "%d", port);

    if (getaddrinfo(host, port_s, &hints, &res) != 0 || !res)
        return -1;

    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) { freeaddrinfo(res); return -1; }

    // Disable Nagle — we manage our own large-chunk writes.
    int yes = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

    // Large kernel send buffer so we never stall waiting for ACKs.
    int sndbuf = SOCK_SNDBUF_SZ;
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

    // Prevent SIGPIPE on closed connections (macOS needs SO_NOSIGPIPE).
#ifdef SO_NOSIGPIPE
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &yes, sizeof(yes));
#endif

    if (connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
        close(fd); freeaddrinfo(res); return -1;
    }
    freeaddrinfo(res);
    return fd;
}

// Query "SIZE\n" from the server.  Returns false on failure.
static bool query_canvas_size(const char *host, int port,
                               int *out_w, int *out_h)
{
    int fd = tcp_connect(host, port);
    if (fd < 0) return false;

    const char cmd[] = "SIZE\n";
#ifdef MSG_NOSIGNAL
    ssize_t s = send(fd, cmd, sizeof(cmd) - 1, MSG_NOSIGNAL);
#else
    ssize_t s = send(fd, cmd, sizeof(cmd) - 1, 0);
#endif
    if (s <= 0) { close(fd); return false; }

    char buf[64] = {};
    // Read enough for "SIZE 65535 65535\n"
    ssize_t r = recv(fd, buf, sizeof(buf) - 1, 0);
    close(fd);
    if (r <= 0) return false;

    return sscanf(buf, "SIZE %d %d", out_w, out_h) == 2;
}

// ─────────────────────────────────────────────────────────────────────────────
//  Worker thread — one TCP connection, blast the buffer in an infinite loop
// ─────────────────────────────────────────────────────────────────────────────

static void *worker_thread(void *vp)
{
    const WorkerArg &a = *static_cast<WorkerArg *>(vp);
    int fd = -1;

    while (g_run) {
        // Connect (or reconnect after an error).
        if (fd < 0) {
            fd = tcp_connect(a.host, a.port);
            if (fd < 0) { usleep(250'000); continue; }
        }

        // Blast the entire buffer; partial sends are handled in the inner loop.
        const char *ptr = a.buf;
        const char *end = a.buf + a.len;

        while (ptr < end && g_run) {
#ifdef MSG_NOSIGNAL
            ssize_t n = send(fd, ptr, (size_t)(end - ptr), MSG_NOSIGNAL);
#else
            ssize_t n = send(fd, ptr, (size_t)(end - ptr), 0);
#endif
            if (n <= 0) { close(fd); fd = -1; break; }
            ptr += n;
            g_bytes.fetch_add((uint64_t)n, std::memory_order_relaxed);
        }
        // Buffer sent completely — loop back and send it again.
    }

    if (fd >= 0) close(fd);
    return nullptr;
}

// ─────────────────────────────────────────────────────────────────────────────
//  Stats thread — print throughput to stderr once per second
// ─────────────────────────────────────────────────────────────────────────────

static void *stats_thread(void *)
{
    uint64_t prev = 0;
    while (g_run) {
        sleep(1);
        const uint64_t cur  = g_bytes.load(std::memory_order_relaxed);
        const double   mbs  = (double)(cur - prev) / 1.0e6;
        const double   totg = (double)cur           / 1.0e9;
        fprintf(stderr,
                "\r  Throughput: %8.2f MB/s   Total: %6.2f GB  ",
                mbs, totg);
        fflush(stderr);
        prev = cur;
    }
    fputc('\n', stderr);
    return nullptr;
}

// ─────────────────────────────────────────────────────────────────────────────
//  main
// ─────────────────────────────────────────────────────────────────────────────

int main(int argc, char **argv)
{
    if (argc < 6) {
        fprintf(stderr,
            "Usage: %s <host> <port> <image.png> <center_x> <center_y> [threads=%d]\n"
            "\n"
            "  host/port    Pixelflut server\n"
            "  image.png    Source PNG (RGBA — transparent pixels are skipped)\n"
            "  center_x/y   Canvas position of the image centre\n"
            "  threads      Parallel TCP connections (default %d)\n",
            argv[0], DEFAULT_THREADS, DEFAULT_THREADS);
        return 1;
    }

    const char *host     = argv[1];
    const int   port     = atoi(argv[2]);
    const char *imgfile  = argv[3];
    const int   cx       = atoi(argv[4]);
    const int   cy       = atoi(argv[5]);
    int         nthreads = (argc >= 7) ? atoi(argv[6]) : DEFAULT_THREADS;

    if (nthreads < 1)           nthreads = 1;
    if (nthreads > MAX_THREADS) nthreads = MAX_THREADS;

    // ── Query canvas size ─────────────────────────────────────────────────────
    int canvas_w = 65535, canvas_h = 65535;
    if (query_canvas_size(host, port, &canvas_w, &canvas_h))
        printf("Canvas: %d × %d px\n", canvas_w, canvas_h);
    else
        fprintf(stderr, "Warning: could not query canvas size — assuming unlimited\n");

    // ── Load image ────────────────────────────────────────────────────────────
    int img_w = 0, img_h = 0, src_channels = 0;
    uint8_t *img = stbi_load(imgfile, &img_w, &img_h, &src_channels, 4);
    if (!img) {
        fprintf(stderr, "Failed to load '%s': %s\n", imgfile,
                stbi_failure_reason());
        return 1;
    }
    printf("Image:  '%s'  %d × %d px  (source channels: %d)\n",
           imgfile, img_w, img_h, src_channels);

    // Canvas top-left of the image (may be negative — clipped below).
    const int x0 = cx - img_w / 2;
    const int y0 = cy - img_h / 2;
    printf("Placed: canvas origin (%d, %d) — (%d, %d)\n",
           x0, y0, x0 + img_w, y0 + img_h);

    // ── Collect non-transparent, in-bounds pixels ─────────────────────────────
    struct Pix { uint16_t x, y; uint8_t r, g, b, a; };
    std::vector<Pix> pixels;
    pixels.reserve((size_t)img_w * img_h);

    for (int py = 0; py < img_h; ++py) {
        const uint8_t *row = img + (size_t)py * img_w * 4;
        for (int px = 0; px < img_w; ++px) {
            const uint8_t *p = row + px * 4;
            if (p[3] == 0) continue; // fully transparent

            const int sx = x0 + px;
            const int sy = y0 + py;

            // Clip to canvas bounds
            if (sx < 0 || sy < 0)         continue;
            if (sx >= canvas_w || sy >= canvas_h) continue;

            pixels.push_back({ (uint16_t)sx, (uint16_t)sy,
                                p[0], p[1], p[2], p[3] });
        }
    }
    stbi_image_free(img);

    const size_t total_px = pixels.size();
    printf("Pixels: %zu visible / %d total  (%.1f%%)\n",
           total_px, img_w * img_h,
           100.0 * (double)total_px / (img_w * img_h));

    if (total_px == 0) {
        fprintf(stderr, "Nothing to send — image is entirely transparent or out of bounds.\n");
        return 1;
    }

    // ── Build command buffers ─────────────────────────────────────────────────
    // "PX 65535 65535 RRGGBBAA\n" = 24 bytes (worst case)
    static constexpr size_t CMD_MAX = 24;

    const int nbufs = std::min(nthreads, DISTINCT_BUFS);
    std::vector<char *> bufs(nbufs);
    std::vector<size_t> lens(nbufs);

    std::mt19937 rng(std::random_device{}());

    for (int b = 0; b < nbufs; ++b) {
        // Independent shuffle per buffer → different pixel delivery order.
        auto pix = pixels;
        std::shuffle(pix.begin(), pix.end(), rng);

        char *mem = new char[pix.size() * CMD_MAX + 1];
        char *ptr = mem;
        for (const auto &px : pix)
            ptr = write_px_cmd(ptr, px.x, px.y, px.r, px.g, px.b, px.a);

        bufs[b] = mem;
        lens[b] = (size_t)(ptr - mem);
        printf("Buffer %d: %.2f MiB\n", b, (double)lens[b] / (1 << 20));
    }

    // ── Signals ───────────────────────────────────────────────────────────────
    signal(SIGINT,  on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN); // handled via MSG_NOSIGNAL / SO_NOSIGPIPE

    // ── Launch worker threads ─────────────────────────────────────────────────
    std::vector<WorkerArg> args(nthreads);
    std::vector<pthread_t> tids(nthreads + 1 /* +1 for stats */);

    for (int i = 0; i < nthreads; ++i) {
        args[i] = { host, port, bufs[i % nbufs], lens[i % nbufs] };
        if (pthread_create(&tids[i], nullptr, worker_thread, &args[i]) != 0) {
            fprintf(stderr, "pthread_create failed for thread %d: %s\n",
                    i, strerror(errno));
            return 1;
        }
    }
    if (pthread_create(&tids[nthreads], nullptr, stats_thread, nullptr) != 0) {
        fprintf(stderr, "pthread_create failed for stats thread\n");
    }

    printf("Running %d thread(s)  —  Ctrl+C to stop\n", nthreads);

    for (int i = 0; i <= nthreads; ++i)
        pthread_join(tids[i], nullptr);

    // ── Cleanup ───────────────────────────────────────────────────────────────
    for (int b = 0; b < nbufs; ++b)
        delete[] bufs[b];

    printf("\nTotal sent: %.3f GB\n", (double)g_bytes.load() / 1.0e9);
    return 0;
}

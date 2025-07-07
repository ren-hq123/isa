#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <stdint.h>
#include <string.h>
#include <float.h>
#include <arpa/inet.h>
#include <stdarg.h>
#include <endian.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

// // 判断主机字节序，定义小端转主机端序的宏
// #if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
// #define le32toh(x) (x)
// #define le16toh(x) (x)
// #else
// #define le32toh(x) OSSwapLittleToHostInt32(x)
// #define le16toh(x) OSSwapLittleToHostInt16(x)
// #endif

// 调试模式开关（1-开启，0-关闭）
//#define DEBUG 0

#define SCALE 1e7 

// 经纬度转米的比例因子（伦敦地区）
static const double LONDON_LON_METER_PER_DEG = 69172.0; // 经度每度对应的米数
static const double LAT_METER_PER_DEG = 111320.0;       // 纬度每度对应的米数
static const double DEFAULT_RADIUS_SQ = 50.0 * 50.0;    // 默认查询半径的平方（米）

static double LON_SCALE; // 经度缩放因子
static double LAT_SCALE; // 纬度缩放因子

#pragma pack(push, 1)
// 文件头结构体
typedef struct {
    char ident[4];         // 文件标识符，应为"DLY8"
    float min_lon;         // 区域最小经度
    float max_lon;         // 区域最大经度
    float min_lat;         // 区域最小纬度
    float max_lat;         // 区域最大纬度
    uint32_t grid_cols;    // 网格列数
    uint32_t grid_rows;    // 网格行数
    uint16_t version;      // 文件版本号
} FileHeader; // 文件头结构体

// 道路元数据结构体
typedef struct {
    uint32_t road_id;      // 道路ID
    uint16_t line_idx;     // 线段索引
    uint8_t speed;         // 限速信息
    uint8_t legend;        // 图例类型
    uint32_t num_points;   // 该道路包含的点数
    int32_t mbr_min_lon;   // 道路最小经度（MBR）
    int32_t mbr_max_lon;   // 道路最大经度（MBR）
    int32_t mbr_min_lat;   // 道路最小纬度（MBR）
    int32_t mbr_max_lat;   // 道路最大纬度（MBR）
    uint8_t avas_combined; // AVAS信息（白天/夜间）
} RoadMeta; // 道路元数据

// 坐标点结构体，存储经纬度（放大为整数）
typedef struct {
    int32_t lon; // 经度
    int32_t lat; // 纬度
} Point; // 坐标点

// 网格单元结构体
// 每个网格单元存储其坐标、包含的道路索引数量及偏移
typedef struct {
    uint16_t x;      // 网格x坐标
    uint16_t y;      // 网格y坐标
    uint32_t count;  // 该网格内道路索引数量
    uint32_t offset; // all_indices数组中的偏移
} GridCell; // 网格单元
#pragma pack(pop)

// 二分查找网格单元，返回目标网格在数组中的下标
static int find_grid_binary_search(GridCell* cells, uint32_t count, uint16_t target_x, uint16_t target_y) {
    int left = 0, right = count - 1;
    while (left <= right) {
        int mid = left + (right - left) / 2;
        GridCell* cell = &cells[mid];
        
        if (cell->x < target_x) left = mid + 1;
        else if (cell->x > target_x) right = mid - 1;
        else {
            if (cell->y < target_y) left = mid + 1;
            else if (cell->y > target_y) right = mid - 1;
            else return mid;
        }
    }
    return -1;
}

// 道路网络主结构体，包含所有数据的指针和元信息
// 通过mmap映射整个二进制文件，指针直接指向文件内存
typedef struct {
    RoadMeta* metas;        // 道路元数据数组
    Point** road_points;    // 每条道路的点数组指针
    uint32_t road_count;    // 道路总数
    GridCell* grid_cells;   // 网格单元数组
    uint32_t grid_count;    // 网格单元总数
    uint32_t* all_indices;  // 所有道路索引数组
    float min_lon, max_lon, min_lat, max_lat; // 区域边界
    uint32_t grid_cols_net, grid_rows_net;    // 网格行列数
    void* __mmap_base;      // mmap映射的基地址
    size_t __mmap_size;     // mmap映射的大小
} RoadNetwork; // 道路网络主结构体

// 查询结果结构体，存储道路索引及距离平方
typedef struct {
    uint32_t index;         // 道路索引
    double distance_sq;     // 距离平方（米）
} RoadResult; // 查询结果

// AVAS信息解析宏
#define AVAS_DAY(combined) ((combined) & 0x0F)           // 白天AVAS
#define AVAS_NIGHT(combined) (((combined) >> 4) & 0x0F)  // 夜间AVAS

// // 调试打印函数
// void debug_print(const char* format, ...) {
// #if DEBUG
//     va_list args;
//     va_start(args, format);
//     vprintf(format, args);
//     va_end(args);
// #endif
// }

// 格式化限速信息，特殊值转为字符串
const char* format_speed_limit(uint8_t speed) {
    static char speed_str[16];
    switch (speed) {
        case 251: return "MISSING";
        case 252: return "NM";
        case 253: return "ND";
        case 254: return "NS";
        case 255: return "ERR";
        default:
            snprintf(speed_str, sizeof(speed_str), "%d", speed);
            return speed_str;
    }
}

// 格式化AVAS信息，特殊值转为字符串
const char* format_avas(uint8_t avas_value) {
    static char avas_str[4];
    switch (avas_value) {
        case 14: return "M";
        case 15: return "E";
        default:
            snprintf(avas_str, sizeof(avas_str), "%d", avas_value);
            return avas_str;
    }
}

// 计算两点间距离的平方
static inline double fast_distance_sq_meter(int32_t lon1, int32_t lat1, int32_t lon2, int32_t lat2) {
    double dx = (lon2 - lon1) * LON_SCALE;
    double dy = (lat2 - lat1) * LAT_SCALE;
    return dx * dx + dy * dy;
}

// 计算点到线段的最短距离的平方
double point_to_segment_meter(int32_t px, int32_t py, int32_t x1, int32_t y1, int32_t x2, int32_t y2) {
    double dx = (x2 - x1) * LON_SCALE;
    double dy = (y2 - y1) * LAT_SCALE;

    const double len_sq = dx * dx + dy * dy;
    if (len_sq == 0.0) { // 线段退化为一个点
        return fast_distance_sq_meter(px, py, x1, y1);
    }

    // 一次乘法代替除法
    const double inv_len_sq = 1.0 / len_sq;
    double t = ((px - x1) * LON_SCALE * dx + (py - y1) * LAT_SCALE * dy) * inv_len_sq;
    
    t = fmax(0.0, fmin(1.0, t)); // 限制t在[0,1]区间
    double nearest_x = x1 + t * (x2 - x1);
    double nearest_y = y1 + t * (y2 - y1);
    return fast_distance_sq_meter(px, py, (int32_t)nearest_x, (int32_t)nearest_y);
}

// 加载二进制道路数据文件，使用mmap映射到内存
// 返回RoadNetwork结构体指针
RoadNetwork* load_binary_data(const char* filename) {
    int fd = open(filename, O_RDONLY);
    if (fd < 0) { perror("[ERROR] 文件打开失败"); return NULL; }
    struct stat st;
    if (fstat(fd, &st) < 0) { perror("[ERROR] 获取文件大小失败"); close(fd); return NULL; }
    size_t filesize = st.st_size;

    // 使用PROT_READ以只读方式映射文件
    void* map_base = mmap(NULL, filesize, PROT_READ, MAP_PRIVATE, fd, 0);
    if (map_base == MAP_FAILED) { perror("[ERROR] mmap失败"); close(fd); return NULL; }
    close(fd);

    uint8_t* ptr = (uint8_t*)map_base;
    RoadNetwork* net = calloc(1, sizeof(RoadNetwork));
    if (!net) { munmap(map_base, filesize); return NULL; }
    
    FileHeader* header = (FileHeader*)ptr;
    if (strncmp(header->ident, "DLY8", 4) != 0) {
        fprintf(stderr, "[ERROR] 不支持的文件格式: %.4s\n", header->ident);
        munmap(map_base, filesize); free(net); return NULL;
    }
    
    // 解析文件头并转换字节序
    // 小端机器上le**toh宏无开销
    net->min_lon = *(float*)&(uint32_t){le32toh(*(uint32_t*)&header->min_lon)};
    net->max_lon = *(float*)&(uint32_t){le32toh(*(uint32_t*)&header->max_lon)};
    net->min_lat = *(float*)&(uint32_t){le32toh(*(uint32_t*)&header->min_lat)};
    net->max_lat = *(float*)&(uint32_t){le32toh(*(uint32_t*)&header->max_lat)};
    net->grid_cols_net = le32toh(header->grid_cols);
    net->grid_rows_net = le32toh(header->grid_rows);
    ptr += sizeof(FileHeader);

    // 读取道路数量
    uint32_t road_count = le32toh(*(uint32_t*)ptr);
    net->road_count = road_count;
    ptr += sizeof(uint32_t);

    // 读取道路元数据数组
    net->metas = (RoadMeta*)ptr;
    ptr += road_count * sizeof(RoadMeta);

    // 读取所有点的数量
    uint32_t total_points = le32toh(*(uint32_t*)ptr);
    ptr += sizeof(uint32_t);
    Point* vertices = (Point*)ptr;
    ptr += total_points * sizeof(Point);

    // 构建每条道路的点数组指针
    net->road_points = malloc(road_count * sizeof(Point*));
    if (!net->road_points) { munmap(map_base, filesize); free(net); return NULL; }
    uint32_t current_point_offset = 0;
    for (uint32_t i = 0; i < road_count; i++) {
        net->road_points[i] = &vertices[current_point_offset];
        current_point_offset += le32toh(net->metas[i].num_points);
    }

    // 读取网格单元数量
    net->grid_count = le32toh(*(uint32_t*)ptr);
    ptr += sizeof(uint32_t);
    
    // 读取网格单元数组
    net->grid_cells = (GridCell*)ptr;
    ptr += net->grid_count * sizeof(GridCell);
    
    // 读取所有道路索引数组
    net->all_indices = (uint32_t*)ptr;
    
    net->__mmap_base = map_base;
    net->__mmap_size = filesize;
    
    return net;
}

// 查询结果排序函数，按距离升序排列
int compare_results(const void* a, const void* b) {
    const RoadResult* ra = a;
    const RoadResult* rb = b;
    return (ra->distance_sq > rb->distance_sq) - (ra->distance_sq < rb->distance_sq);
}

// 查询指定点附近的道路，按距离排序输出
void query_roads(RoadNetwork* net, double query_lon, double query_lat, double radius_m, uint32_t max_results) {
    int32_t query_lon_fixed = (int32_t)(query_lon * SCALE); // 查询点经度（整数）
    int32_t query_lat_fixed = (int32_t)(query_lat * SCALE); // 查询点纬度（整数）
    
    // 计算经纬度范围
    const double delta_lon = radius_m / LONDON_LON_METER_PER_DEG;
    const double delta_lat = radius_m / LAT_METER_PER_DEG;
    
    const int32_t delta_lon_fixed = (int32_t)(delta_lon * SCALE);
    const int32_t delta_lat_fixed = (int32_t)(delta_lat * SCALE);

    const double q_min_lon = query_lon - delta_lon;
    const double q_max_lon = query_lon + delta_lon;
    const double q_min_lat = query_lat - delta_lat;
    const double q_max_lat = query_lat + delta_lat;

    // 计算网格范围
    const float grid_step = 0.01f;
    int x_min = fmax(0, fmin((q_min_lon - net->min_lon) / grid_step, net->grid_cols_net-1));
    int x_max = fmax(0, fmin((q_max_lon - net->min_lon) / grid_step, net->grid_cols_net-1));
    int y_min = fmax(0, fmin((q_min_lat - net->min_lat) / grid_step, net->grid_rows_net-1));
    int y_max = fmax(0, fmin((q_max_lat - net->min_lat) / grid_step, net->grid_rows_net-1));

    uint32_t raw_candidate_count = 0; // 候选道路总数
    for (int x = x_min; x <= x_max; x++) {
        for (int y = y_min; y <= y_max; y++) {
            int grid_idx = find_grid_binary_search(net->grid_cells, net->grid_count, (uint16_t)x, (uint16_t)y);
            if (grid_idx >= 0) {
                raw_candidate_count += net->grid_cells[grid_idx].count;
            }
        }
    }

    if (raw_candidate_count == 0) {
        printf("\n查询结果（显示前%d条）：\n", max_results);
        return;
    }
    
    RoadResult* results = malloc(raw_candidate_count * sizeof(RoadResult)); // 查询结果数组
    uint8_t* visited = calloc(net->road_count, sizeof(uint8_t));           // 标记已访问道路
    uint32_t valid_results = 0; // 有效结果数量

    // 遍历相关网格，收集候选道路并计算距离
    for (int x = x_min; x <= x_max; x++) {
        for (int y = y_min; y <= y_max; y++) {
            int grid_idx = find_grid_binary_search(net->grid_cells, net->grid_count, (uint16_t)x, (uint16_t)y);
            if (grid_idx >= 0) {
                GridCell* cell = &net->grid_cells[grid_idx];
                for (uint32_t i = 0; i < cell->count; i++) {
                    const uint32_t idx = net->all_indices[cell->offset + i];
                    if (idx >= net->road_count || visited[idx]) continue; // 跳过越界或已访问
                    visited[idx] = 1;
                    RoadMeta* meta = &net->metas[idx];
                    
                    //MBR与查询范围相交检查
                    if (query_lon_fixed < meta->mbr_min_lon - delta_lon_fixed ||
                        query_lon_fixed > meta->mbr_max_lon + delta_lon_fixed ||
                        query_lat_fixed < meta->mbr_min_lat - delta_lat_fixed ||
                        query_lat_fixed > meta->mbr_max_lat + delta_lat_fixed) {
                        continue;
                    }
                    //计算点到道路的最小距离
                    double min_dist_sq = DBL_MAX;
                    for (uint32_t p = 0; p < meta->num_points - 1; p++) {
                        Point* p1 = &net->road_points[idx][p];
                        Point* p2 = &net->road_points[idx][p+1];
                        double dist_sq = point_to_segment_meter(query_lon_fixed, query_lat_fixed, p1->lon, p1->lat, p2->lon, p2->lat);
                        if (dist_sq < min_dist_sq) min_dist_sq = dist_sq;
                    }
                    if (min_dist_sq <= DEFAULT_RADIUS_SQ) {
                        results[valid_results++] = (RoadResult){idx, min_dist_sq};
                    }
                }
            }
        }
    }

    // 按距离排序
    qsort(results, valid_results, sizeof(RoadResult), compare_results);

    printf("\n查询结果（显示前%d条）：\n", max_results);
    for (uint32_t i = 0; i < valid_results && i < max_results; i++) {
        RoadMeta* meta = &net->metas[results[i].index];
        double actual_distance = sqrt(results[i].distance_sq);
        
        printf("%2d. 道路ID:%-8u 距离:%6.2fm 限速:%-8s Legend:%-3d AVAS:[%s,%s]\n",
              i+1, meta->road_id, actual_distance, format_speed_limit(meta->speed), 
              meta->legend, format_avas(AVAS_DAY(meta->avas_combined)), format_avas(AVAS_NIGHT(meta->avas_combined)));
    }

    free(visited);
    free(results);
}

// 释放内存和资源，解除mmap映射
void free_network(RoadNetwork* net) {
    if (!net) return;
    // 释放指针数组
    if (net->road_points) free(net->road_points);
    // 解除整个内存区域的映射
    if (net->__mmap_base) munmap(net->__mmap_base, net->__mmap_size);
    // 释放主结构体本身
    free(net);
}

int main() {
    // 初始化经纬度缩放因子
    LON_SCALE = LONDON_LON_METER_PER_DEG / SCALE;
    LAT_SCALE = LAT_METER_PER_DEG / SCALE;
    printf("Loading binary data via mmap...\n");
    RoadNetwork* net = load_binary_data("data.bin");
    if (!net) return EXIT_FAILURE;
    
    // 进入查询
    printf("Data loaded and ready for queries.\n");
    // 查询点数组
    double query_points[1][2] = {
       //测试点
         {-0.000467,51.539443},//真实
         };
    int num_points = 1;      // 查询点数量
    double radius = 50.0;    // 查询半径（米）
    int max_results = 5;     // 最多显示结果数

    // 主循环，持续查询
    while (1) {
        for (int i = 0; i < num_points; i++) {
            query_roads(net, query_points[i][0], query_points[i][1], radius, max_results);
        }
        usleep(10000); 
   }

 //for (int i = 0; i < num_points; i++) {
       //printf("\n==== 查询点%d: (%.8f, %.8f) ====\n", i+1, query_points[i][0], query_points[i][1]);
       //query_roads(net, query_points[i][0], query_points[i][1], radius, max_results);
    //}

    free_network(net);
    return EXIT_SUCCESS;
}
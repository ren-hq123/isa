#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <stdint.h>
#include <string.h>
#include <float.h>
#include <arpa/inet.h>
#include <stdarg.h>
#include <libkern/OSByteOrder.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdbool.h>

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define le32toh(x) (x)
#define le16toh(x) (x)
#else
#define le32toh(x) OSSwapLittleToHostInt32(x)
#define le16toh(x) OSSwapLittleToHostInt16(x)
#endif

// Configuration parameters
#define MATCH_TRIGGER_SECONDS 5.0
#define MATCH_TRIGGER_METERS 30.0
#define CONFIDENCE_THRESHOLD_PERCENTAGE 0.35
#define FAST_PATH_THRESHOLD 0.70 // Lowered from 0.85
#define TOP_N_CANDIDATES 4
#define LATEST_POINT_WEIGHT_MULTIPLIER 2.0
#define CANDIDATE_SEARCH_RADIUS 35.0 // Increased from 30.0
#define MIN_DISTANCE_THRESHOLD 0.5
#define SCALE 1e7
static const double LONDON_LON_METER_PER_DEG = 69172.0;
static const double LAT_METER_PER_DEG = 111320.0;
static double LON_SCALE, LAT_SCALE;

// Data structures
#pragma pack(push, 1)
typedef struct {
   char ident[4];
   float min_lon, max_lon, min_lat, max_lat;
   uint32_t grid_cols, grid_rows;
   uint16_t version;
} FileHeader;

typedef struct {
   uint32_t road_id;
   uint16_t line_idx;
   uint8_t speed, legend;
   uint32_t num_points;
   int32_t mbr_min_lon, mbr_max_lon, mbr_min_lat, mbr_max_lat;
   uint8_t avas_combined;
} RoadMeta;

typedef struct {
   int32_t lon, lat;
} Point;

typedef struct {
   uint16_t x, y;
   uint32_t count, offset;
} GridCell;
#pragma pack(pop)

typedef struct {
   RoadMeta* metas;
   Point** road_points;
   uint32_t road_count;
   GridCell* grid_cells;
   uint32_t grid_count;
   uint32_t* all_indices;
   float min_lon, max_lon, min_lat, max_lat;
   uint32_t grid_cols_net, grid_rows_net;
   void* __mmap_base;
   size_t __mmap_size;
} RoadNetwork;

typedef struct {
   double lon, lat, timestamp;
} GPSPoint;

typedef struct {
   GPSPoint points[6];
   int point_count;
   double cumulative_distance, start_time;
   uint8_t current_speed_limit, backup_speed_limit;
   uint32_t last_matched_road_idx;
} BusLimitDetector;

typedef struct {
   uint32_t road_idx;
   uint8_t speed_limit;
   double total_vote_weight;
} RoadVote;

typedef struct {
   RoadVote votes[5];
   int unique_count;
} VotingResult;

typedef struct {
   uint32_t idx;
   double dist_sq;
} Candidate;

// Global variables
static int total_decision_cycles = 0;
static int total_passes = 0;
static int fast_path_passes = 0;
static int shortcut_passes = 0;
static BusLimitDetector global_detector = {0};
static RoadNetwork* net = NULL;

// Function prototypes
double calculate_gps_distance(GPSPoint* p1, GPSPoint* p2);
const char* format_avas(uint8_t avas_value);
uint8_t simple_bus_matching(BusLimitDetector* detector, uint32_t* winning_road_idx);
double perform_weighted_voting(BusLimitDetector* detector, VotingResult* result, uint32_t single_road_idx_to_check);

#define AVAS_DAY(combined) ((combined) & 0x0F)
#define AVAS_NIGHT(combined) (((combined) >> 4) & 0x0F)

static int find_grid_binary_search(GridCell* cells, uint32_t count, uint16_t target_x, uint16_t target_y) {
   int left = 0, right = count - 1;
   while (left <= right) {
       int mid = left + (right - left) / 2;
       GridCell* cell = &cells[mid];
       if (cell->x < target_x || (cell->x == target_x && cell->y < target_y)) left = mid + 1;
       else if (cell->x > target_x || (cell->x == target_x && cell->y > target_y)) right = mid - 1;
       else return mid;
   }
   return -1;
}

static inline double fast_distance_sq_meter(int32_t lon1, int32_t lat1, int32_t lon2, int32_t lat2) {
   double dx = (double)(lon2 - lon1) * LON_SCALE;
   double dy = (double)(lat2 - lat1) * LAT_SCALE;
   return dx * dx + dy * dy;
}

double point_to_segment_meter_sq(int32_t px, int32_t py, int32_t x1, int32_t y1, int32_t x2, int32_t y2) {
    double dx = (double)(x2 - x1) * LON_SCALE;
    double dy = (double)(y2 - y1) * LAT_SCALE;
    double len_sq = dx * dx + dy * dy;
    if (len_sq < 1e-6) {
        return fast_distance_sq_meter(px, py, x1, y1);
    }
    double t = (((double)(px - x1) * LON_SCALE * dx) + ((double)(py - y1) * LAT_SCALE * dy)) / len_sq;
    t = fmax(0.0, fmin(1.0, t));
    double nearest_x = (double)x1 + t * (double)(x2 - x1);
    double nearest_y = (double)y1 + t * (double)(y2 - y1);
    return fast_distance_sq_meter(px, py, (int32_t)round(nearest_x), (int32_t)round(nearest_y));
}

int compare_candidates(const void* a, const void* b) {
    const Candidate* cand_a = (const Candidate*)a;
    const Candidate* cand_b = (const Candidate*)b;
    if (cand_a->dist_sq < cand_b->dist_sq) return -1;
    if (cand_a->dist_sq > cand_b->dist_sq) return 1;
    return 0;
}

static int compare_road_votes(const void* a, const void* b) {
    const RoadVote* vote_a = (const RoadVote*)a;
    const RoadVote* vote_b = (const RoadVote*)b;
    if (vote_a->total_vote_weight > vote_b->total_vote_weight) return -1;
    if (vote_a->total_vote_weight < vote_b->total_vote_weight) return 1;
    return 0;
}

RoadNetwork* load_binary_data(const char* filename) {
    printf("[Debug] Loading map file: %s\n", filename);
    int fd = open(filename, O_RDONLY);
    if (fd < 0) {
        perror("[Debug] Failed to open map file");
        return NULL;
    }
    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        perror("[Debug] Failed to stat map file");
        return NULL;
    }
    size_t filesize = st.st_size;
    void* map_base = mmap(NULL, filesize, PROT_READ, MAP_PRIVATE, fd, 0);
    if (map_base == MAP_FAILED) {
        close(fd);
        perror("[Debug] Failed to mmap map file");
        return NULL;
    }
    close(fd);
    uint8_t* ptr = (uint8_t*)map_base;
    RoadNetwork* net = calloc(1, sizeof(RoadNetwork));
    if (!net) {
        munmap(map_base, filesize);
        printf("[Debug] Error: Failed to allocate RoadNetwork\n");
        return NULL;
    }
    FileHeader* header = (FileHeader*)ptr;
    if (strncmp(header->ident, "DLY8", 4) != 0) {
        munmap(map_base, filesize);
        free(net);
        printf("[Debug] Error: Invalid map file format (ident != DLY8)\n");
        return NULL;
    }
    net->min_lon = *(float*)&(uint32_t){le32toh(*(uint32_t*)&header->min_lon)};
    net->max_lon = *(float*)&(uint32_t){le32toh(*(uint32_t*)&header->max_lon)};
    net->min_lat = *(float*)&(uint32_t){le32toh(*(uint32_t*)&header->min_lat)};
    net->max_lat = *(float*)&(uint32_t){le32toh(*(uint32_t*)&header->max_lat)};
    net->grid_cols_net = le32toh(header->grid_cols);
    net->grid_rows_net = le32toh(header->grid_rows);
    ptr += sizeof(FileHeader);
    uint32_t road_count = le32toh(*(uint32_t*)ptr);
    net->road_count = road_count;
    if (road_count == 0 || road_count > 1000000) {
        munmap(map_base, filesize);
        free(net);
        printf("[Debug] Error: Invalid road_count %u\n", road_count);
        return NULL;
    }
    ptr += sizeof(uint32_t);
    net->metas = (RoadMeta*)ptr;
    ptr += road_count * sizeof(RoadMeta);
    uint32_t total_points = le32toh(*(uint32_t*)ptr);
    if (total_points == 0 || total_points > 10000000) {
        munmap(map_base, filesize);
        free(net);
        printf("[Debug] Error: Invalid total_points %u\n", total_points);
        return NULL;
    }
    ptr += sizeof(uint32_t);
    Point* vertices = (Point*)ptr;
    ptr += total_points * sizeof(Point);
    net->road_points = malloc(road_count * sizeof(Point*));
    if (!net->road_points) {
        munmap(map_base, filesize);
        free(net);
        printf("[Debug] Error: Failed to allocate road_points\n");
        return NULL;
    }
    uint32_t current_point_offset = 0;
    for (uint32_t i = 0; i < road_count; i++) {
        uint32_t num_points = le32toh(net->metas[i].num_points);
        if (num_points == 0 || current_point_offset + num_points > total_points) {
            munmap(map_base, filesize);
            free(net->road_points);
            free(net);
            printf("[Debug] Error: Invalid num_points %u for road %u\n", num_points, i);
            return NULL;
        }
        net->road_points[i] = &vertices[current_point_offset];
        current_point_offset += num_points;
    }
    net->grid_count = le32toh(*(uint32_t*)ptr);
    if (net->grid_count == 0 || net->grid_count > net->grid_cols_net * net->grid_rows_net) {
        munmap(map_base, filesize);
        free(net->road_points);
        free(net);
        printf("[Debug] Error: Invalid grid_count %u\n", net->grid_count);
        return NULL;
    }
    ptr += sizeof(uint32_t);
    net->grid_cells = (GridCell*)ptr;
    ptr += net->grid_count * sizeof(GridCell);
    net->all_indices = (uint32_t*)ptr;
    net->__mmap_base = map_base;
    net->__mmap_size = filesize;
    printf("[Debug] Map loaded: %u roads, %u points, %u grid cells\n", net->road_count, total_points, net->grid_count);
    return net;
}

void free_network(RoadNetwork* net) {
   if (!net) return;
   if (net->road_points) free(net->road_points);
   if (net->__mmap_base) munmap(net->__mmap_base, net->__mmap_size);
   free(net);
   printf("[Debug] Map cleaned up\n");
}

double calculate_gps_distance(GPSPoint* p1, GPSPoint* p2) {
   double dx = (p2->lon - p1->lon) * LONDON_LON_METER_PER_DEG;
   double dy = (p2->lat - p1->lat) * LAT_METER_PER_DEG;
   return sqrt(dx * dx + dy * dy);
}

void add_gps_point(BusLimitDetector* detector, GPSPoint new_point) {
   printf("[Debug] Entering add_gps_point: (%.6f, %.6f)\n", new_point.lon, new_point.lat);
   if (detector->point_count > 0) {
       GPSPoint* last_point = &detector->points[detector->point_count - 1];
       double dist = calculate_gps_distance(last_point, &new_point);
       if (dist < MIN_DISTANCE_THRESHOLD) {
           printf("[Debug] Skipping repeated GPS point: (%.6f, %.6f), distance=%.2fm\n", new_point.lon, new_point.lat, dist);
           return;
       }
   }
   if (detector->point_count >= 6) {
       memmove(detector->points, detector->points + 1, 5 * sizeof(GPSPoint));
       detector->points[5] = new_point;
   } else {
       detector->points[detector->point_count] = new_point;
       detector->point_count++;
   }
   if (detector->point_count == 1) {
       detector->start_time = new_point.timestamp;
       detector->cumulative_distance = 0.0;
   } else {
       double segment_dist = calculate_gps_distance(&detector->points[detector->point_count - 2], &detector->points[detector->point_count - 1]);
       detector->cumulative_distance += segment_dist;
   }
   printf("[Debug] Added GPS point: (%.6f, %.6f), point_count=%d, cumulative_distance=%.2fm\n",
          new_point.lon, new_point.lat, detector->point_count, detector->cumulative_distance);
}

static bool should_process(BusLimitDetector* detector, double current_time) {
   if (detector->point_count < 2) return false;
   double elapsed_time = current_time - detector->start_time;
   bool result = elapsed_time >= MATCH_TRIGGER_SECONDS && detector->cumulative_distance >= MATCH_TRIGGER_METERS;
   printf("[Debug] should_process: elapsed_time=%.2fs, cumulative_distance=%.2fm, process=%d\n",
          elapsed_time, detector->cumulative_distance, result);
   return result;
}

int find_nearest_candidates(GPSPoint* point, Candidate* candidates, int n) {
    if (!net || !net->metas || !net->road_points || !net->grid_cells || !net->all_indices) {
        printf("[Debug] Error: RoadNetwork not initialized\n");
        return 0;
    }
    
    printf("[Debug] Entering find_nearest_candidates: (%.6f, %.6f)\n", point->lon, point->lat);
    
    int32_t query_lon_fixed = (int32_t)(point->lon * SCALE);
    int32_t query_lat_fixed = (int32_t)(point->lat * SCALE);
    
    const float grid_step = 0.01f;
    
    // 计算GPS点所在的网格坐标
    int grid_x = (int)floor((point->lon - net->min_lon) / grid_step);
    int grid_y = (int)floor((point->lat - net->min_lat) / grid_step);
    
    // 边界检查
    if (grid_x < 0 || grid_x >= net->grid_cols_net || 
        grid_y < 0 || grid_y >= net->grid_rows_net) {
        printf("[Debug] GPS point outside grid bounds\n");
        return 0;
    }
    
    // MBR检查的安全边距
    const double radius_m = CANDIDATE_SEARCH_RADIUS;
    const double delta_lon_rad = radius_m / LONDON_LON_METER_PER_DEG;
    const double delta_lat_rad = radius_m / LAT_METER_PER_DEG;
    const int32_t delta_lon_fixed = (int32_t)(delta_lon_rad * SCALE);
    const int32_t delta_lat_fixed = (int32_t)(delta_lat_rad * SCALE);
    
    static uint32_t query_id = 0;
    static uint32_t* visited_roads = NULL;
    if (!visited_roads) {
        visited_roads = calloc(net->road_count, sizeof(uint32_t));
        if (!visited_roads) {
            printf("[Debug] Error: Failed to allocate visited_roads\n");
            return 0;
        }
    }
    query_id++;
    
    Candidate potential_candidates[100];
    int potential_count = 0;
    
    // 搜索单个网格
    printf("[Debug] Searching single grid cell: x=%d, y=%d\n", grid_x, grid_y);
    int grid_idx = find_grid_binary_search(net->grid_cells, net->grid_count, grid_x, grid_y);
    
    if (grid_idx >= 0) {
        GridCell* cell = &net->grid_cells[grid_idx];
        printf("[Debug] Found grid with %u roads\n", cell->count);
        
        for (uint32_t i = 0; i < cell->count; i++) {
            uint32_t idx = net->all_indices[cell->offset + i];
            if (idx >= net->road_count || visited_roads[idx] == query_id) continue;
            visited_roads[idx] = query_id;
            
            RoadMeta* meta = &net->metas[idx];
            if (!meta || !net->road_points[idx]) {
                printf("[Debug] Error: Invalid meta or road_points for road_idx %u\n", idx);
                continue;
            }
            
            // MBR过滤
            if (query_lon_fixed < meta->mbr_min_lon - delta_lon_fixed ||
                query_lon_fixed > meta->mbr_max_lon + delta_lon_fixed ||
                query_lat_fixed < meta->mbr_min_lat - delta_lat_fixed ||
                query_lat_fixed > meta->mbr_max_lat + delta_lat_fixed) {
                continue;
            }
            
            // 距离计算（保持原有逻辑）
            double min_dist_sq = DBL_MAX;
            if (meta->num_points > 1) {
                for (uint32_t p = 0; p < meta->num_points - 1; p++) {
                    Point* p1 = &net->road_points[idx][p];
                    Point* p2 = &net->road_points[idx][p + 1];
                    double dist_sq = point_to_segment_meter_sq(query_lon_fixed, query_lat_fixed, 
                                                             p1->lon, p1->lat, p2->lon, p2->lat);
                    if (dist_sq < min_dist_sq) min_dist_sq = dist_sq;
                }
            } else if (meta->num_points == 1) {
                min_dist_sq = fast_distance_sq_meter(query_lon_fixed, query_lat_fixed, 
                                                   net->road_points[idx][0].lon, net->road_points[idx][0].lat);
            }
            
            const double radius_m_sq = radius_m * radius_m;
            if (min_dist_sq <= radius_m_sq && potential_count < 100) {
                potential_candidates[potential_count].idx = idx;
                potential_candidates[potential_count].dist_sq = min_dist_sq;
                potential_count++;
                printf("[Debug] Candidate %d: road_id=%u, dist=%.2fm\n", 
                       potential_count-1, net->metas[idx].road_id, sqrt(min_dist_sq));
            }
        }
    } else {
        printf("[Debug] No grid cell found at x=%d, y=%d\n", grid_x, grid_y);
    }
    
    // 排序和返回（保持原有逻辑）
    qsort(potential_candidates, potential_count, sizeof(Candidate), compare_candidates);
    int num_to_copy = fmin(n, potential_count);
    for (int k = 0; k < num_to_copy; k++) {
        memcpy(&candidates[k], &potential_candidates[k], sizeof(Candidate));
    }
    
    printf("[Debug] Found %d candidates for point (%.6f, %.6f)\n", num_to_copy, point->lon, point->lat);
    return num_to_copy;
}

double perform_weighted_voting(BusLimitDetector* detector, VotingResult* result, uint32_t single_road_idx_to_check) {
    if (!net || !net->metas || !net->road_points) {
        printf("[Debug] Error: RoadNetwork not initialized\n");
        return 0.0;
    }
    printf("[Debug] Entering perform_weighted_voting: single_road_idx=%u\n", single_road_idx_to_check);
    result->unique_count = 0;
    memset(result->votes, 0, sizeof(result->votes));
    double total_weight_cast = 0;
    for (int i = 0; i < detector->point_count; i++) {
        Candidate top_candidates[TOP_N_CANDIDATES];
        int num_candidates = find_nearest_candidates(&detector->points[i], top_candidates, TOP_N_CANDIDATES);
        if (num_candidates == 0) {
            printf("[Debug] Warning: No candidates found for point %d (%.6f, %.6f)\n", i, detector->points[i].lon, detector->points[i].lat);
            continue;
        }
        double time_weight = (i == detector->point_count - 1) ? LATEST_POINT_WEIGHT_MULTIPLIER : 1.0;
        for (int k = 0; k < num_candidates; k++) {
            uint32_t road_idx = top_candidates[k].idx;
            if (single_road_idx_to_check != UINT32_MAX && road_idx != single_road_idx_to_check) continue;
            if (road_idx >= net->road_count || !net->metas[road_idx].speed) {
                printf("[Debug] Error: Invalid road_idx %u or speed\n", road_idx);
                continue;
            }
            double dist_weight = 1.0 / (1.0 + sqrt(top_candidates[k].dist_sq) / 5.0); // Tighter decay
            double final_weight = dist_weight * time_weight;
            total_weight_cast += final_weight;
            int found_idx = -1;
            for (int j = 0; j < result->unique_count; j++) {
                if (result->votes[j].road_idx == road_idx) {
                    found_idx = j;
                    break;
                }
            }
            if (found_idx >= 0) {
                result->votes[found_idx].total_vote_weight += final_weight;
            } else if (result->unique_count < 5) {
                result->votes[result->unique_count].road_idx = road_idx;
                result->votes[result->unique_count].speed_limit = net->metas[road_idx].speed;
                result->votes[result->unique_count].total_vote_weight = final_weight;
                result->unique_count++;
            }
        }
    }
    if (single_road_idx_to_check != UINT32_MAX && result->unique_count == 0) {
        printf("[Debug] Warning: No candidates matched single_road_idx %u\n", single_road_idx_to_check);
    }
    printf("[Debug] Weighted voting: total_weight=%.2f, unique_count=%d\n", total_weight_cast, result->unique_count);
    return total_weight_cast;
}

uint8_t simple_bus_matching(BusLimitDetector* detector, uint32_t* winning_road_idx) {
    if (!net || !net->metas || !net->road_points) {
        printf("[Debug] Error: RoadNetwork not initialized\n");
        return 0;
    }
    printf("[Debug] Entering simple_bus_matching\n");
    total_decision_cycles++;
    *winning_road_idx = UINT32_MAX;
    VotingResult voting = {0};
    double total_weight_cast = perform_weighted_voting(detector, &voting, UINT32_MAX);
    if (voting.unique_count == 0 || total_weight_cast < 1e-6) {
        printf("[Debug] No valid candidates in voting\n");
        return 0;
    }
    qsort(voting.votes, voting.unique_count, sizeof(RoadVote), compare_road_votes);
    if (voting.unique_count >= 2) {
        RoadVote* winner = &voting.votes[0];
        RoadVote* runner_up = &voting.votes[1];
        if (winner->speed_limit == runner_up->speed_limit) {
            printf("[Shortcut Path] Winner/Runner-up speeds match (%u mph). Confidence ACCEPTED.\n", winner->speed_limit);
            shortcut_passes++;
            total_passes++;
            *winning_road_idx = winner->road_idx;
            return winner->speed_limit;
        }
    }
    printf("[Confidence Check] Using standard support rate check...\n");
    RoadVote* winner = &voting.votes[0];
    double total_weight_for_winning_camp = 0;
    for (int i = 0; i < voting.unique_count; i++) {
        if (voting.votes[i].speed_limit == winner->speed_limit) {
            total_weight_for_winning_camp += voting.votes[i].total_vote_weight;
        }
    }
    double support_rate = total_weight_for_winning_camp / total_weight_cast;
    printf("[Debug] Support rate: %.2f%% (threshold %.2f%%)\n", support_rate * 100.0, CONFIDENCE_THRESHOLD_PERCENTAGE * 100.0);
    if (support_rate >= CONFIDENCE_THRESHOLD_PERCENTAGE) {
        total_passes++;
        *winning_road_idx = winner->road_idx;
        return winner->speed_limit;
    }
    printf("[Debug] No valid match in simple_bus_matching\n");
    return 0;
}

void update_backup_speed_limit(BusLimitDetector* detector) {
   if (detector->point_count < 2) return;
   double total_time = detector->points[detector->point_count-1].timestamp - detector->start_time;
   if (total_time > 1.0) {
       double avg_speed_kph = (detector->cumulative_distance / total_time) * 3.6;
       if (avg_speed_kph > 65.0) detector->backup_speed_limit = 40;
       else if (avg_speed_kph > 45.0) detector->backup_speed_limit = 30;
       else detector->backup_speed_limit = 20;
       printf("[Debug] Backup speed limit updated: %u mph (speed %.2f kph)\n",
              detector->backup_speed_limit, avg_speed_kph);
   }
}

void reset_detector(BusLimitDetector* detector) {
   detector->point_count = 0;
   detector->cumulative_distance = 0.0;
   detector->start_time = 0.0;
   //detector->last_matched_road_idx = UINT32_MAX;
   printf("[Debug] Detector reset\n");
}

uint8_t get_bus_speed_limit(double lon, double lat, double timestamp) {
    if (!net || !net->metas || !net->road_points) {
        printf("[Debug] Error: RoadNetwork not initialized\n");
        return global_detector.backup_speed_limit;
    }
    printf("[Debug] Entering get_bus_speed_limit: (%.6f, %.6f, %.1f)\n", lon, lat, timestamp);
    GPSPoint new_point = {lon, lat, timestamp};
    add_gps_point(&global_detector, new_point);
    if (global_detector.point_count == 0) {
        printf("[Debug] Returning speed limit (filtered point): %u mph\n", global_detector.current_speed_limit ? global_detector.current_speed_limit : global_detector.backup_speed_limit);
        return global_detector.current_speed_limit ? global_detector.current_speed_limit : global_detector.backup_speed_limit;
    }
    uint8_t result = global_detector.current_speed_limit ? global_detector.current_speed_limit : global_detector.backup_speed_limit;
    if (should_process(&global_detector, timestamp)) {
        uint32_t matched_road_idx = UINT32_MAX;
        uint8_t new_limit = 0;
        bool fast_path_taken = false;
        if (global_detector.last_matched_road_idx != UINT32_MAX && global_detector.last_matched_road_idx < net->road_count) {
            VotingResult historical_result = {0};
            double total_weight = perform_weighted_voting(&global_detector, &historical_result, global_detector.last_matched_road_idx);
            if (total_weight > 1e-6 && historical_result.unique_count > 0) {
                double support_rate = historical_result.votes[0].total_vote_weight / total_weight;
                RoadMeta* meta = &net->metas[global_detector.last_matched_road_idx];
                printf("[Fast Path Check] SupportRate: %.2f%% for History -> 道路ID:%u 距离:---m 限速:%u Legend:%u AVAS:[%s,%s]\n",
                       support_rate * 100.0, meta->road_id, meta->speed, meta->legend,
                       format_avas(AVAS_DAY(meta->avas_combined)), format_avas(AVAS_NIGHT(meta->avas_combined)));
                if (support_rate > FAST_PATH_THRESHOLD) {
                    new_limit = meta->speed;
                    matched_road_idx = global_detector.last_matched_road_idx;
                    fast_path_taken = true;
                    fast_path_passes++;
                    printf("[Debug] Fast Path succeeded: road %u, speed %u mph\n", meta->road_id, new_limit);
                } else {
                    printf("[Debug] Fast Path failed: support_rate %.2f%% < %.2f%%\n", support_rate * 100.0, FAST_PATH_THRESHOLD * 100.0);
                    global_detector.last_matched_road_idx = UINT32_MAX;
                }
            } else {
                printf("[Debug] Fast Path failed: total_weight=%.2f, unique_count=%d\n", total_weight, historical_result.unique_count);
                global_detector.last_matched_road_idx = UINT32_MAX;
            }
        }
        if (!fast_path_taken) {
            printf("[Debug] Fast Path FAILED/SKIPPED. Performing full search...\n");
            new_limit = simple_bus_matching(&global_detector, &matched_road_idx);
        }
        if (new_limit > 0 && new_limit <= 80) {
            global_detector.current_speed_limit = new_limit;
            if (matched_road_idx != UINT32_MAX) {
                global_detector.last_matched_road_idx = matched_road_idx;
                printf("[Debug] Matched road %u, speed limit %u mph\n", net->metas[matched_road_idx].road_id, new_limit);
            }
        }
        reset_detector(&global_detector);
        result = global_detector.current_speed_limit ? global_detector.current_speed_limit : global_detector.backup_speed_limit;
    }
    update_backup_speed_limit(&global_detector);
    printf("[Debug] Returning speed limit: %u mph\n", result);
    return result;
}

const char* format_avas(uint8_t avas_value) {
    static char avas_str[4];
    if (avas_value == 14) return "M";
    if (avas_value == 15) return "E";
    snprintf(avas_str, sizeof(avas_str), "%d", avas_value);
    return avas_str;
}

const char* format_speed_limit(uint8_t speed) {
    static char speed_str[16];
    switch (speed) {
        case 0: return "Matching...";
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

int main() {
    LON_SCALE = LONDON_LON_METER_PER_DEG / SCALE;
    LAT_SCALE = LAT_METER_PER_DEG / SCALE;
    printf("[Debug] Scaling constants: LON_SCALE=%.6f, LAT_SCALE=%.6f\n", LON_SCALE, LAT_SCALE);
    net = load_binary_data("/Users/florrif/Desktop/test0701/data.bin");
    if (!net) {
        perror("Failed to load data.bin");
        return EXIT_FAILURE;
    }
    global_detector.backup_speed_limit = 20;
    global_detector.last_matched_road_idx = UINT32_MAX;
    printf("[Debug] Initial backup speed limit: %u mph\n", global_detector.backup_speed_limit);
    double gps_trajectory[][3] = {
        {0.060962, 51.498797, 1.0},
        {0.060962, 51.498797, 2.0},
        {0.060896, 51.498769, 3.0},
        {0.060830, 51.498752, 4.0},
        {0.060770, 51.498736, 5.0},
        {0.060648, 51.498760, 6.0},
        {0.060648, 51.498760, 7.0},
        {0.060601, 51.498788, 8.0},
        {0.060553, 51.498862, 9.0},
        {0.060536, 51.498895, 10.0},
        {0.060536, 51.498895, 11.0},
        {0.060589, 51.498920, 12.0},
        {0.060589, 51.498920, 13.0},
        {0.060636, 51.498950, 14.0},
        {0.060636, 51.498950, 15.0},
        {0.060738, 51.498976, 16.0},
        {0.060738, 51.498976, 17.0},
        {0.060738, 51.498976, 18.0},
        {0.060799, 51.498984, 19.0},
        {0.060864, 51.498991, 20.0},
        {0.060928, 51.498993, 21.0},
        {0.060992, 51.498995, 22.0},
        {0.061108, 51.498991, 23.0},
        {0.061172, 51.498986, 24.0},
        {0.061172, 51.498986, 25.0},
        {0.061248, 51.498983, 26.0},
        {0.061328, 51.498989, 27.0},
        {0.061421, 51.498994, 28.0},
        {0.061530, 51.498998, 29.0},
        {0.061643, 51.498995, 30.0},
        {0.061765, 51.498983, 31.0},
        {0.061984, 51.498956, 32.0},
        {0.062096, 51.498926, 33.0},
        {0.062096, 51.498926, 34.0},
        {0.062193, 51.498913, 35.0},
        {0.062281, 51.498898, 36.0},
        {0.062387, 51.498876, 37.0},
        {0.062481, 51.498854, 38.0},
        {0.062584, 51.498829, 39.0},
        {0.062679, 51.498818, 40.0},
        {0.062780, 51.498822, 41.0},
        {0.062894, 51.498831, 42.0},
        {0.063106, 51.498854, 43.0},
        {0.063204, 51.498867, 44.0},
        {0.063204, 51.498867, 45.0},
        {0.063297, 51.498881, 46.0},
        {0.063367, 51.498908, 47.0},
        {0.063452, 51.498987, 48.0},
        {0.063496, 51.499032, 49.0},
        {0.063496, 51.499032, 50.0},
        {0.063551, 51.499137, 51.0},
        {0.063556, 51.499193, 52.0},
        {0.063540, 51.499271, 53.0},
        {0.063526, 51.499335, 54.0},
        {0.063526, 51.499335, 55.0},
        {0.063512, 51.499403, 56.0},
        {0.063491, 51.499472, 57.0},
        {0.063482, 51.499546, 58.0},
        {0.063471, 51.499604, 59.0},
        {0.063464, 51.499667, 60.0},
        {0.063433, 51.499787, 61.0},
        {0.063433, 51.499787, 62.0},
        {0.063411, 51.499899, 63.0},
        {0.063411, 51.499899, 64.0},
        {0.063404, 51.499947, 65.0},
        {0.063396, 51.499976, 66.0},
        {0.063392, 51.500017, 67.0},
        {0.063392, 51.500017, 68.0}
    };
    int num_points = sizeof(gps_trajectory) / sizeof(gps_trajectory[0]);
    printf("[Debug] Processing %d GPS points\n", num_points);
    for (int i = 0; i < num_points; i++) {
        uint8_t speed_limit = get_bus_speed_limit(gps_trajectory[i][0], gps_trajectory[i][1], gps_trajectory[i][2]);
        printf("GPS点%d: (%.6f, %.6f) -> 限速: %s mph\n", i + 1,
               gps_trajectory[i][0], gps_trajectory[i][1], format_speed_limit(speed_limit));
        usleep(100000);
    }
    printf("\n--- 运行时分析 ---\n");
    printf("总决策次数: %d\n", total_decision_cycles);
    printf("通过置信度检查次数: %d\n", total_passes);
    if (total_decision_cycles > 0) {
        printf("置信度检查通过率: %.2f%%\n", (double)total_passes / total_decision_cycles * 100.0);
    }
    printf("快捷路径通过次数: %d\n", shortcut_passes);
    printf("快速路径通过次数: %d\n", fast_path_passes);
    printf("--------------------\n");
    free_network(net);
    return EXIT_SUCCESS;
}
import struct
import math
import json
import os
import logging
from collections import defaultdict

# 配置参数
GRID_STEP = 0.01
BUFFER = 0.01
MAX_LINE_INDEX = 65535
SCALE = 1e7

# 日志配置
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("road_processor.log"),
        logging.StreamHandler()
    ]
)

# 更新地理边界
def update_bounds(bounds, segment):
    """更新地理边界"""
    bounds["min_lon"] = min(bounds["min_lon"], segment.min_lon)
    bounds["max_lon"] = max(bounds["max_lon"], segment.max_lon)
    bounds["min_lat"] = min(bounds["min_lat"], segment.min_lat)
    bounds["max_lat"] = max(bounds["max_lat"], segment.max_lat)

# 解析速度限制
def parse_speed_limit(speed_limit_str):
    """解析速度限制值，处理特殊情况"""
    speed_str = str(speed_limit_str).strip().upper()
    # 处理数字值
    if speed_str.isdigit():
        return min(255, max(0, int(speed_str)))
    # 处理特殊值
    special_speed_values = {"ND": 253, "NS": 254, "NM": 252}
    if speed_str in special_speed_values:
        logging.debug(f"检测到特殊速度值: {speed_limit_str} -> {special_speed_values[speed_str]}")
        return special_speed_values[speed_str]
    # 其他未知情况返回默认值
    logging.warning(f"未知的速度限制值: {speed_limit_str}, 使用默认值251")
    return 251

# 处理坐标数据
def process_coordinates(coords):
    """处理坐标数据，确保格式正确"""
    processed = []
    for coord in coords:
        try:
            if len(coord) >= 2:
                lon, lat = float(coord[0]), float(coord[1])
                # 基本的坐标范围检查
                if -180 <= lon <= 180 and -90 <= lat <= 90:
                    processed.append((lon, lat))
                else:
                    logging.warning(f"坐标超出范围: [{lon}, {lat}]")
        except (ValueError, TypeError, IndexError) as e:
            logging.warning(f"无效坐标: {coord}, 错误: {e}")
            continue
    return processed

# 路段对象
class RoadSegment:
    def __init__(self, road_id, line_idx, is_multiline, speed, points, 
                 legend=None, avas_step_day=0, avas_step_night=0, original_toid=None):
        # 字段范围校验
        if not (0 <= line_idx <= MAX_LINE_INDEX):
            raise ValueError(f"line_idx {line_idx} 超出范围（0-{MAX_LINE_INDEX}）")
        if not (0 <= speed <= 255):
            raise ValueError(f"speed {speed} 超出范围（0-255）")
        # 确保road_id在32位无符号整数范围内
        if road_id < 0:
            road_id = abs(road_id) % (2**32)
        elif road_id > 0xFFFFFFFF:  # 2**32 - 1
            original_id = road_id
            road_id = road_id % (2**32)
            logging.warning(f"road_id {original_id} 超出32位范围，自动转换为 {road_id}")
        # AVAS字段校验
        if not (0 <= avas_step_day <= 15):
            raise ValueError(f"avas_step_day {avas_step_day} 超出范围（0-15）")
        if not (0 <= avas_step_night <= 15):
            raise ValueError(f"avas_step_night {avas_step_night} 超出范围（0-15）")
        # Legend字段校验
        if legend is not None and not (0 <= legend <= 255):
            raise ValueError(f"legend {legend} 超出范围（0-255）")
        self.road_id = int(road_id)
        self.line_idx = int(line_idx)
        self.is_multiline = bool(is_multiline)
        self.speed = int(speed)
        self.points = points
        self.num_points = len(points)
        self.legend = legend if isinstance(legend, int) else 0
        self.avas_step_day = int(avas_step_day)
        self.avas_step_night = int(avas_step_night)
        self.original_toid = original_toid or ""  # 保存原始TOID
        # MBR计算
        if self.num_points < 2:
            raise ValueError("路段至少需要2个坐标点")
        lons = [p[0] for p in points]
        lats = [p[1] for p in points]
        self.min_lon, self.max_lon = min(lons), max(lons)
        self.min_lat, self.max_lat = min(lats), max(lats)
        logging.debug(f"创建路段: 内部ID={self.road_id} 原始TOID={self.original_toid} "
                     f"Legend={self.legend} 包含{self.num_points}个点 "
                     f"AVAS=[{self.avas_step_day},{self.avas_step_night}]")

# 处理GeoJSON文件，生成路段列表
class GeoJSONProcessor:
    @staticmethod
    def process_geojson_file(input_file):
        """处理标准GeoJSON格式文件"""
        all_segments = []
        geo_bounds = {"min_lon": 180.0, "max_lon": -180.0, "min_lat": 90.0, "max_lat": -90.0}
        logging.info(f"开始处理GeoJSON文件: {input_file}")
        with open(input_file, "r", encoding="utf-8") as f:
            geojson_data = json.load(f)
        if not isinstance(geojson_data, dict) or geojson_data.get("type") != "FeatureCollection":
            raise ValueError("输入文件不是有效的GeoJSON FeatureCollection格式")
        features = geojson_data.get("features", [])
        logging.info(f"找到 {len(features)} 个要素")
        # 动态构建道路类型映射
        legend_to_num = {}
        next_legend_id = 1
        for feature_idx, feature in enumerate(features):
            try:
                if feature.get("type") != "Feature":
                    logging.warning(f"跳过非Feature类型的要素: {feature_idx}")
                    continue
                geometry = feature.get("geometry", {})
                properties = feature.get("properties", {})
                # 提取属性
                toid = properties.get("TOID", str(feature_idx))
                legend_str = properties.get("Legend") or "" 
                speed_limit = properties.get("Speed_Limit", "251")
                avas_step_day = properties.get("AvasStepDay", "14")
                avas_step_night = properties.get("AvasStepNight", "14")
                #Legend字段：检测TOID错误填充
                if legend_str.startswith("osgb") and not legend_str.isdigit() and len(legend_str) > 10:
                    logging.warning(f"检测到Legend字段包含TOID值: {legend_str}，已清理为特殊标识'CLEANED_INVALID_TOID'")
                    legend_str = "INVALID"
                # 动态分配legend数字ID
                if legend_str not in legend_to_num:
                    legend_to_num[legend_str] = next_legend_id
                    next_legend_id += 1
                # 数据清理和转换
                try:
                    # 处理TOID，但为每个Feature生成唯一的road_id
                    # 即使TOID相同，也要区分不同的Feature
                    if isinstance(toid, str) and toid.isdigit():
                        base_id = int(toid) % (2**31)  # 使用31位避免溢出
                    elif isinstance(toid, str):
                        base_id = abs(hash(toid)) % (2**31)
                    else:
                        base_id = int(toid) % (2**31)
                    # 使用feature_idx确保每个Feature都有唯一的road_id
                    # 即使TOID相同，也不重复
                    road_id = (base_id * 1000000 + feature_idx) % (2**32)
                    # 速度解析
                    speed = parse_speed_limit(speed_limit)
                    avas_day = min(15, int(avas_step_day)) if str(avas_step_day).isdigit() else 14
                    avas_night = min(15, int(avas_step_night)) if str(avas_step_night).isdigit() else 14
                    # 使用动态映射获取数字值
                    legend_num = legend_to_num[legend_str]
                    # 记录原始TOID用于调试
                    logging.debug(f"Feature {feature_idx}: 原始TOID={toid} -> road_id={road_id}")
                except (ValueError, TypeError) as e:
                    logging.warning(f"属性转换错误 (要素{feature_idx}): {e}, 使用异常值标记")
                    road_id = feature_idx % (2**32)
                    speed = 255
                    avas_day = 15
                    avas_night = 15
                    
                    error_legend_str = "__ERROR__" 
                    if error_legend_str not in legend_to_num:
                        legend_to_num[error_legend_str] = next_legend_id
                        next_legend_id += 1
                    legend_num = legend_to_num[error_legend_str]

                # 处理几何数据
                geometry_type = geometry.get("type")
                coordinates = geometry.get("coordinates", [])
                if geometry_type == "MultiLineString":
                    for line_idx, line_coords in enumerate(coordinates):
                        processed_points = process_coordinates(line_coords)
                        if len(processed_points) >= 2:
                            segment = RoadSegment(
                                road_id=road_id, line_idx=line_idx, is_multiline=True,
                                speed=speed, points=processed_points, legend=legend_num,  
                                avas_step_day=avas_day, avas_step_night=avas_night,
                                original_toid=str(toid)
                            )
                            update_bounds(geo_bounds, segment)
                            all_segments.append(segment)
                elif geometry_type == "LineString":
                    processed_points = process_coordinates(coordinates)
                    if len(processed_points) >= 2:
                        segment = RoadSegment(
                            road_id=road_id, line_idx=0, is_multiline=False,
                            speed=speed, points=processed_points, legend=legend_num,  
                            avas_step_day=avas_day, avas_step_night=avas_night,
                            original_toid=str(toid)
                        )
                        update_bounds(geo_bounds, segment)
                        all_segments.append(segment)
                else:
                    logging.warning(f"不支持的几何类型: {geometry_type} (要素{feature_idx})")
                    continue
            except Exception as e:
                logging.error(f"处理要素{feature_idx}失败: {str(e)}")
                logging.debug(f"错误要素内容: {feature}")
                continue
        logging.info(f"成功处理 {len(all_segments)} 个路段")
        logging.info(f"发现 {len(legend_to_num)} 种不同的Legend类型: {dict(legend_to_num)}")
        return all_segments, geo_bounds, legend_to_num

# 构建网格索引
class GridBuilder:
    @staticmethod
    def build_grid(segments, geo_bounds):
        epsilon = 1e-9
        grid_cols = math.ceil((geo_bounds["max_lon"] - geo_bounds["min_lon"]) / GRID_STEP)
        grid_rows = math.ceil((geo_bounds["max_lat"] - geo_bounds["min_lat"]) / GRID_STEP)
        grid_map = defaultdict(list)
        for seg_idx, seg in enumerate(segments):
            min_x = math.floor((seg.min_lon - geo_bounds["min_lon"]) / GRID_STEP)
            max_x = math.floor((seg.max_lon - geo_bounds["min_lon"] - epsilon) / GRID_STEP)
            min_y = math.floor((seg.min_lat - geo_bounds["min_lat"]) / GRID_STEP)
            max_y = math.floor((seg.max_lat - geo_bounds["min_lat"] - epsilon) / GRID_STEP)
            min_x = max(0, min(min_x, grid_cols-1))
            max_x = max(0, min(max_x, grid_cols-1))
            min_y = max(0, min(min_y, grid_rows-1))
            max_y = max(0, min(max_y, grid_rows-1))
            logging.debug(f"路段{seg_idx} 网格覆盖: X[{min_x}-{max_x}] Y[{min_y}-{max_y}]")
            for x in range(min_x, max_x+1):
                for y in range(min_y, max_y+1):
                    grid_map[(x, y)].append(seg_idx)
        logging.info(f"生成网格: {len(grid_map)}个单元 (共{grid_cols}x{grid_rows}网格)")
        return grid_map, grid_cols, grid_rows

# 写二进制文件
class BinaryWriter:
    @staticmethod
    def write(output_file, all_segments, geo_bounds, grid_map, grid_cols, grid_rows):
        try:
            with open(output_file, "wb") as f:
                header = struct.pack(
                    "<4s4f2IH", b"DLY8", geo_bounds["min_lon"], geo_bounds["max_lon"],
                    geo_bounds["min_lat"], geo_bounds["max_lat"], grid_cols, grid_rows, 1
                )
                f.write(header)
                f.write(struct.pack("<I", len(all_segments)))
                meta_format = "<IHBBI4iB"
                for seg in all_segments:
                    avas_combined = (seg.avas_step_day << 4) | (seg.avas_step_night & 0x0F)
                    meta = struct.pack(
                        meta_format, seg.road_id, seg.line_idx, seg.speed, seg.legend,
                        seg.num_points, int(round(seg.min_lon * SCALE)),
                        int(round(seg.max_lon * SCALE)), int(round(seg.min_lat * SCALE)),
                        int(round(seg.max_lat * SCALE)), avas_combined
                    )
                    f.write(meta)
                total_points = sum(seg.num_points for seg in all_segments)
                f.write(struct.pack("<I", total_points))
                logging.info(f"顶点数据总数: {total_points}")
                for seg in all_segments:
                    for lon, lat in seg.points:
                        f.write(struct.pack("<2i", int(round(lon*SCALE)), int(round(lat*SCALE))))
                f.write(struct.pack("<I", len(grid_map)))
                  #处理网格与索引数据
                sorted_grid_keys = sorted(grid_map.keys())
                grid_cell_data_to_write = []
                all_indices_to_write = []
                current_offset = 0
                
                for x, y in sorted_grid_keys:
                    indices = grid_map[(x, y)]
                    num_indices = len(indices)
                    #当前要写入的 GridCell 结构体包含偏移量
                    #格式：(x, y, 计数, 偏移量)
                    cell_struct = struct.pack("<HHI I", x, y, num_indices, current_offset)
                    grid_cell_data_to_write.append(cell_struct)
                    #将该单元的索引添加到主列表  
                    all_indices_to_write.extend(indices)
                    #更新下一个单元的偏移量  
                    current_offset += num_indices

                #写入连续的GridCell数据块 
                for cell_data in grid_cell_data_to_write:
                    f.write(cell_data)
                #写入整合后的单一索引数据块  
                f.write(struct.pack(f"<{len(all_indices_to_write)}I", *all_indices_to_write))
                logging.info(f"网格索引: {len(grid_map)}个网格单元")
                logging.info(f"总索引数: {len(all_indices_to_write)}")
        except Exception as e:
            logging.error("文件写入失败，正在删除不完整文件...")
            if os.path.exists(output_file):
                os.remove(output_file)
            raise

# 生成映射CSV文件
def generate_csv_mapping_file(all_segments, output_base_path, legend_to_str=None):
    if legend_to_str is None:
        legend_to_str = {}
    csv_file = f"{output_base_path}_road_mapping.csv"
    with open(csv_file, 'w', encoding='utf-8') as f:
        f.write("road_id,original_toid,legend_num,legend_str,speed,avas_day,avas_night,num_points,coordinates\n")
        for seg in all_segments:
            legend_str_val = legend_to_str.get(seg.legend, "Unknown")
            coordinates_str = ";".join([f"({lon},{lat})" for lon, lat in seg.points])
            f.write(f"{seg.road_id},{seg.original_toid},{seg.legend},\"{legend_str_val}\",{seg.speed},{seg.avas_step_day},{seg.avas_step_night},{seg.num_points},\"{coordinates_str}\"\n")
    size_mb = os.path.getsize(csv_file) / (1024 * 1024)
    logging.info(f"生成映射文件: {csv_file} ({size_mb:.1f} MB)")
    return csv_file

# 主流程入口
def main(input_path, output_path):
    try:
        logging.info("=== 开始处理（生成主二进制文件和CSV映射文件）===")
        
        logging.info("本版本仅支持GeoJSON格式输入。")
        segments, geo_bounds, legend_mapping = GeoJSONProcessor.process_geojson_file(input_path)
            
        if not segments:
            raise ValueError("没有有效路段数据")
        geo_bounds = {
            "min_lon": geo_bounds["min_lon"] - BUFFER, "max_lon": geo_bounds["max_lon"] + BUFFER,
            "min_lat": geo_bounds["min_lat"] - BUFFER, "max_lat": geo_bounds["max_lat"] + BUFFER
        }
        logging.info(f"最终地理范围: {geo_bounds}")
        grid_map, cols, rows = GridBuilder.build_grid(segments, geo_bounds)
        if not grid_map:
            raise ValueError("网格索引为空，请检查路段数据")
        BinaryWriter.write(output_path, segments, geo_bounds, grid_map, cols, rows)
        logging.info(f"生成文件: {output_path}")
        file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
        logging.info(f"主文件大小: {file_size_mb:.1f} MB (无映射数据)")
        logging.info(f"统计: {len(segments)}路段 / {sum(s.num_points for s in segments)}顶点")
        output_base = os.path.splitext(output_path)[0]
        legend_to_str = {v: k for k, v in legend_mapping.items()} if legend_mapping else None
        generate_csv_mapping_file(segments, output_base, legend_to_str)
        # 统计日志
        avas_day_stats, avas_night_stats = defaultdict(int), defaultdict(int)
        legend_stats, toid_stats, speed_stats = defaultdict(int), defaultdict(int), defaultdict(int)
        for seg in segments:
            avas_day_stats[seg.avas_step_day] += 1
            avas_night_stats[seg.avas_step_night] += 1
            legend_stats[seg.legend] += 1
            toid_stats[seg.original_toid] += 1
            speed_stats[seg.speed] += 1
        logging.info("=== 数据统计 ===")
        logging.info(f"AVAS白天步数分布: {dict(avas_day_stats)}")
        logging.info(f"AVAS夜间步数分布: {dict(avas_night_stats)}")
        logging.info(f"Legend数字分布: {dict(legend_stats)}")
        speed_stats_display = {}
        for speed, count in speed_stats.items():
            if speed == 251: speed_stats_display["251 (MISSING-缺失数据)"] = count
            elif speed == 252: speed_stats_display["252 (NM-未测量)"] = count
            elif speed == 253: speed_stats_display["253 (ND-无数据)"] = count
            elif speed == 254: speed_stats_display["254 (NS-无限速)"] = count
            elif speed == 255: speed_stats_display["255 (ERROR-异常值)"] = count
            else: speed_stats_display[str(speed)] = count
        logging.info(f"速度限制分布: {speed_stats_display}")
        special_speed_count = sum(speed_stats.get(s, 0) for s in [251, 252, 253, 254, 255])
        if special_speed_count > 0:
            logging.info(f"特殊速度值路段总数: {special_speed_count} (MISSING:{speed_stats.get(251, 0)}, NM:{speed_stats.get(252, 0)}, ND:{speed_stats.get(253, 0)}, NS:{speed_stats.get(254, 0)}, ERROR:{speed_stats.get(255, 0)})")
        avas_day_special = avas_day_stats.get(14, 0) + avas_day_stats.get(15, 0)
        avas_night_special = avas_night_stats.get(14, 0) + avas_night_stats.get(15, 0)
        if avas_day_special > 0 or avas_night_special > 0:
            logging.info(f"AVAS特殊值统计: 白天(14-缺失:{avas_day_stats.get(14, 0)}, 15-异常:{avas_day_stats.get(15, 0)}) 夜间(14-缺失:{avas_night_stats.get(14, 0)}, 15-异常:{avas_night_stats.get(15, 0)})")
        duplicate_toids = {toid: count for toid, count in toid_stats.items() if count > 1}
        unique_toids = len(toid_stats) - len(duplicate_toids)
        if duplicate_toids:
            logging.info(f"TOID统计: {len(duplicate_toids)}个重复TOID, {unique_toids}个唯一TOID")
            sample_duplicates = dict(list(duplicate_toids.items())[:5])
            logging.info(f"重复TOID示例: {sample_duplicates} ...")
        else:
            logging.info(f"TOID统计: 所有{len(toid_stats)}个TOID都是唯一的")
    except Exception as e:
        logging.error(f"处理失败: {str(e)}", exc_info=True)
        exit(1)

if __name__ == "__main__":
    main(
        "data1.json",  # 输入GeoJSON文件
        "data1.bin"  # 输出二进制文件
    )
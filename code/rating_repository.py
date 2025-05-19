

import psycopg2
def getConnection(user='postgres', password='quangphu', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def LoadRatings(ratingsTableName, ratingsFilePath, openConnection):
    """
    Tải dữ liệu từ @ratingsTableName vào cơ sở dữ diệu có tên bảng là @ratingsFilePath
    """
    conn = openConnection
    if not conn:
        return
    
    cursor = conn.cursor()
    
 
    # Tạo bảng @ratingsTableName
    cursor.execute(f"""
    CREATE TABLE {ratingsTableName} (
        UserID INT,
        MovieID INT,
        Rating FLOAT
    )
    """)
    
    # Đọc dữ liệu từ file và insert vào csdl với tên bảng @ratingsTableName
    with open(ratingsFilePath, 'r') as f:
        for line in f:
            parts = line.strip().split('::')
            if len(parts) >= 3:
                user_id = int(parts[0])
                movie_id = int(parts[1])
                rating = float(parts[2])
                
                # Insert vào bảng
                cursor.execute(
                    f"INSERT INTO {ratingsTableName} (UserID, MovieID, Rating) VALUES (%s, %s, %s)",
                    (user_id, movie_id, rating)
                )
    
    conn.commit()
    print(f"Tải dữ liệu từ file {ratingsFilePath} vào bảng {ratingsTableName} thành công.")

def Range_Partition(ratingsTableName, numberPartitions, openConnection):
    """
        Partition bảng @ratingsTableName thành @numberPartition khoảng giá trị đồng đều của thuộc tính rating
    """
    conn = openConnection
    cursor = conn.cursor()
    
    # Lấy giá trị thấp và cao nhất của bảng @ratingsTableName
    cursor.execute(f"SELECT MIN(Rating), MAX(Rating) FROM {ratingsTableName}")
    min_rating, max_rating = cursor.fetchone()
    
    # tính giá trị đồng đều mỗi partition
    range_size = (max_rating - min_rating) / numberPartitions
    
    # Tạo bảng metadata để lưu thông tin về partition
    cursor.execute("DROP TABLE IF EXISTS range_metadata")
    cursor.execute("""
    CREATE TABLE range_metadata (
        partition_id INT,
        min_value FLOAT,
        max_value FLOAT
    )
    """)
    
    # Tại mỗi partition tính các khoảng giá trị
    for i in range(numberPartitions):
        # Tính khoảng giá trị rating
        lower_bound = min_rating + (i * range_size)
        upper_bound = min_rating + ((i + 1) * range_size)
        
        # Nếu partition là partition cuối cùng thì lấy giá trị lớn nhất là max_rating
        if i == numberPartitions - 1:
            upper_bound = max_rating
        
        # Tạo bảng cho partition
        partition_name = f"range_part{i}"
        cursor.execute(f"DROP TABLE IF EXISTS {partition_name}")
        cursor.execute(f"""
        CREATE TABLE {partition_name} (
            UserID INT,
            MovieID INT,
            Rating FLOAT
        )
        """)
        
        # Lưu trữ thông tin về partition
        cursor.execute(
            "INSERT INTO range_metadata VALUES (%s, %s, %s)",
            (i, lower_bound, upper_bound)
        )
        
        # Lưu trữ thông tin rating vào partition tương ứng
        if i == 0:
            # Partition đầu tiên thì cần lưu cả giá trị lower_bound
            cursor.execute(f"""
            INSERT INTO {partition_name}
            SELECT * FROM {ratingsTableName}
            WHERE Rating >= {lower_bound} AND Rating <= {upper_bound}
            """)
        else:
            # Đối với các partiton khác thì loại bỏ lower_bound
            cursor.execute(f"""
            INSERT INTO {partition_name}
            SELECT * FROM {ratingsTableName}
            WHERE Rating > {lower_bound} AND Rating <= {upper_bound}
            """)
    
    conn.commit()
    print(f"Tạo thành công {numberPartitions} partition từ bảng {ratingsTableName} thành công.")

def RoundRobin_Partition(ratingTableName, numberPartition, openConnection):
    pass

def RoundRobin_Insert(ratingTableName, userId, itemId, rating, openConnection):
    pass
def Range_Insert(ratingTableName, userId, itemId, rating, openConnection):
    pass
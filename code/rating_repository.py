

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

def RoundRobin_Partition(ratingsTableName, numberPartitions, openConnection):
    """
        Phân mảnh vòng tròn:
        Có 5 partition:
        rating0 => p0
        rating1 => p1
        rating2 => p2
        rating3 => p3
        rating4 => p4
        rating5 => p0
        rating6 => p1
    """
    conn = openConnection
    cursor = openConnection.cursor()
    """
        Tạo bảng metadata để lưu info về partition
        @partition_count: số lượng phân mảnh
        @next_partition: lưu giá trị partition kế tiếp mà dữ liệu sẽ được lưu vào.
        Ví dụ: next_partition = 1 thì giá trị sẽ lưu vào bảng rrobin_part1
        Nếu next_partition = (số phân mảnh - 1) thì sẽ lưu giá trị 0 => quay trở lại
    """
    cursor.execute("DROP TABLE IF EXISTS rrobin_metadata")
    cursor.execute("""
    CREATE TABLE rrobin_metadata (
        partition_count INT,
        next_partition INT
    )
    """)
    
    # Khời tạo metadata
    cursor.execute("INSERT INTO rrobin_metadata VALUES (%s, %s)", (numberPartitions, 0))

     # Tạo partition table
    for i in range(numberPartitions):
        partition_name = f"rrobin_part{i}"
        cursor.execute(f"DROP TABLE IF EXISTS {partition_name}")
        cursor.execute(f"""
        CREATE TABLE {partition_name} (
            UserID INT,
            MovieID INT,
            Rating FLOAT
        )
        """)
    
     
    cursor.execute(f"SELECT UserID, MovieID, Rating FROM {ratingsTableName}")
    all_rows = cursor.fetchall()
    
    # Insert vào partition tương ứng
    for i, row in enumerate(all_rows):
        partition_index = i % numberPartitions
        partition_name = f"rrobin_part{partition_index}"
        cursor.execute(
            f"INSERT INTO {partition_name} VALUES (%s, %s, %s)",
            (row[0], row[1], row[2])
        )
    
    # Cập nhật partition_next để biết bảng partition nào sẽ lưu giá trị khi chèn vào.
    cursor.execute("UPDATE rrobin_metadata SET next_partition = %s", (len(all_rows) % numberPartitions,))
    conn.commit()
    print(f"Tạo thành công {numberPartitions} phân mảnh vòng tròn.")
    


def RoundRobin_Insert(ratingsTableName, userId, itemId, rating, openConnection):
    conn = openConnection
    cursor = conn.cursor()

    # Chèn vào bảng gốc
    cursor.execute(
        f"INSERT INTO {ratingsTableName} (UserID, MovieID, Rating) VALUES (%s, %s, %s)",
        (userId, itemId, rating)
    )
    
    cursor.execute("SELECT partition_count, next_partition FROM rrobin_metadata")
    partition_count, next_partition = cursor.fetchone()

    partition_name = f"rrobin_part{next_partition}"
    cursor.execute(
        f"INSERT INTO {partition_name} (UserID, MovieID, Rating) VALUES (%s, %s, %s)",
        (userId, itemId, rating)
    )

    # Cập nhật partiton_next
    next_partition = (next_partition + 1) % partition_count
    cursor.execute("UPDATE rrobin_metadata SET next_partition = %s", (next_partition,))

    conn.commit()
    print("Chèn thành công data theo phân mảnh vòng tròn")
def Range_Insert(ratingsTableName, userId, itemId, rating, openConnection):
    conn = getConnection()
    cursor = conn.cursor()
    #Chèn vào bảng gốc
    cursor.execute(
        f"INSERT INTO {ratingsTableName} (UserId, MovieId, Rating) VALUES (%s, %s, %s)",
        (userId, itemId, rating)
    )

    #Lấy metadata của partition trong vùng: (min_value, max_value]
    cursor.execute(
        "SELECT partition_id FROM range_metadata WHERE %s > min_value AND %s <= max_value",
        (rating, rating)
    )

    result = cursor.fetchone()
    # Không thấy partition nằm trong vùng (min_value, max_value]
    if not result:
        # Tìm trong vùng [min_value, max_value]
        cursor.execute(
            "SELECT partition_id FROM range_metadata WHERE %s >= min_value AND %s <= max_value",
            (rating, rating)
        )
        result = cursor.fetchone()

    # Tìm thấy 
    if result:
        partition_id = result[0]
        partition_name = f"range_part{partition_id}"
        
        # Chèn vào partition tương ứng
        cursor.execute(
            f"INSERT INTO {partition_name} (UserID, MovieID, Rating) VALUES (%s, %s, %s)",
            (userId, itemId, rating)
        )

        conn.commit()
        print(f"Đã chèn data vào phân vùng: {partition_name}")
    else:
        conn.rollback()
        print("Error: không tìm thấy phân vùng phù hợp")

    
    
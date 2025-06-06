import random
import datetime

# Function to generate a random timestamp
def random_timestamp(start_year=2000, end_year=2025):
    start = datetime.datetime(start_year, 1, 1)
    end = datetime.datetime(end_year, 12, 31)
    return int(random.uniform(start.timestamp(), end.timestamp()))

formatted_lines = []
for _ in range(100000):
    user_id = random.randint(1, 500)
    movie_id = random.randint(1, 300)
    rating = random.randint(1, 5)
    timestamp = random_timestamp()
    line = f"{user_id}::{movie_id}::{rating}::{timestamp}"
    formatted_lines.append(line)

# Save to file
with open("test_data_range.txt", "w") as file:
    file.write("\n".join(formatted_lines))

print("Data saved to movielen_formatted_data.txt")

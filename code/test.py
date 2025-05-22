import rating_repository as RatingRepository


openConnection = RatingRepository.getConnection()

# RatingRepository.LoadRatings("Rating", "./test_data.dat", openConnection)
# RatingRepository.Range_Partition("Rating", 3, openConnection)
# RatingRepository.RoundRobin_Partition("Rating", 4, openConnection)
# RatingRepository.RoundRobin_Insert("Rating", 5000, 23945, 4.5, openConnection)

RatingRepository.Range_Insert("Rating", 342, 2325235, 2.5, openConnection)
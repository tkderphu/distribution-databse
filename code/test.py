import rating_repository as RatingRepository


openConnection = RatingRepository.getConnection()

# RatingRepository.LoadRatings("Rating", "./test_data.dat", openConnection)
RatingRepository.Range_Partition("Rating", 3, openConnection)
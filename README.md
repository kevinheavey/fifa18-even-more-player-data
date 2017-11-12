# A more complete FIFA 18 player dataset

#### Forked from [here](https://github.com/amanthedorkknight/fifa18-all-player-statistics), but this project no longer bears much resemblance to the original.

This repo contains both the dataset and the code used to scrape so-fifa.com.

See the dataset on Kaggle [here](https://www.kaggle.com/kevinmh/fifa-18-more-complete-player-dataset)

## Why fork the original project?

1. I wanted more fields. This dataset contains extra fields such as International Reputation, traits and specialities.
2. Efficiency:
    - The original project all takes place in a Jupyter notebook. I've rebuilt the crawler as a Python package.
    - The original project runs synchronously. I can only imagine that this takes several hours.
3. I've stored the data using the .feather format alongside .csv - this is more convenient for Python and R users
4. Column names now only contain letters, numbers and underscores. This is safer for most analysis tools
5. Cleanliness: I've pre-cleaned the data to a great extent. Most of this has involved converting strings to numerics (sometimes with some extra leg-work when dealing with inconsistent units)
6. Fun

## Future improvements:

- There are still a few fields that could be added if anyone wants them, such as contract expiry date.
- Scrapy might speed things up
- Tests
- Building an archive (EA updates its player data regularly and it would be useful to be able to make comparisons between old versions).
We can go all the way back to FIFA 2007, and it shouldn't be too hard to do so, although the once-off execution time would be in the order of a few days if we scrape all past versions of the data. 
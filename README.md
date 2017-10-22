# A more complete FIFA 18 player dataset

#### Forked from [here](https://github.com/amanthedorkknight/fifa18-all-player-statistics), but this project no longer bears much resemblance to the original.

This repo contains both the dataset and the code used to scrape so-fifa.com.

## Why fork the original project?

1. I wanted more fields. This dataset contains extra fields such as International Reputation, traits and specialities.
2. Efficiency:
    - The original project all takes place in a Jupyter notebook. I've rebuilt it in .py files.
    - The original project runs synchronously. I can only imagine that this takes several hours.
3. Fun

## Future improvements:

- There are still a few fields that could be added if anyone wants them, such as contract expiry date.
- Scrapy might speed things up
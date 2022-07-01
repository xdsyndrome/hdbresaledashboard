# HDB Resale Prices Dashboard
## About
- This module downloads the HDB Resale Prices data from gov.sg.
- User is required to provide the MRT coordinates data (available from Kaggle.com).
- Coordinates of HDB flats will be obtained from the OneMap API.

## How to Run

- For first run:
```
python src --download=True
```

- If data is available in /data:
```
python src --download=False
```

## Roadmap
- [ ] Add cache
- [ ] Improve config file to include Kaggle URL. 
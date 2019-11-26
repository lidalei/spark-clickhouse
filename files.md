# Dimensional modeling

## Reviews

```shell
curl -I https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/item_dedup.json.gz
```

```text
HTTP/1.1 200 OK
x-amz-id-2: 0EbUdtgbEa6EoA8FOHmja88N0CghxjR5LfMRMvCdqIKHJSSbjMQ0eo9rZju/C1O1VIhckkaZ5PM=
x-amz-request-id: DB1A5D89CF1057E5
Date: Fri, 22 Nov 2019 10:19:29 GMT
Last-Modified: Fri, 15 Jan 2016 10:35:17 GMT
ETag: "8caf1a04ba08f82781e7bc01cfd47896-2283"
Accept-Ranges: bytes
Content-Type: application/json
Content-Length: 19150810764
Server: AmazonS3
```

### Sample review

```json
{
  "reviewerID": "A2SUAM1J3GNN3B",
  "asin": "0000013714",
  "reviewerName": "J. McDonald",
  "helpful": [2, 3],
  "reviewText": "I bought this for my husband who plays the piano.  He is having a wonderful time playing these old hymns.  The music  is at times hard to read because we think the book was published for singing from more than playing from.  Great purchase though!",
  "overall": 5.0,
  "summary": "Heavenly Highway Hymns",
  "unixReviewTime": 1252800000,
  "reviewTime": "09 13, 2009"
}
```

where

```text
reviewerID - ID of the reviewer, e.g. A2SUAM1J3GNN3B
asin - ID of the product, e.g. 0000013714
reviewerName - name of the reviewer
helpful - helpfulness rating of the review, e.g. 2/3
reviewText - text of the review
overall - rating of the product
summary - summary of the review
unixReviewTime - time of the review (unix time)
reviewTime - time of the review (raw)
```

## Metadata

```shell
curl -I https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/metadata.json.gz
```

```text
HTTP/1.1 200 OK
x-amz-id-2: VKlr1Vmy6Q3EPuNYRZfv1BKSd75Z65NhyMbmtR4RoqCWTF+Cgq5JxZeYxQL9YH5Jmlvqlbakdz4=
x-amz-request-id: 0BFE07A65B37D854
Date: Fri, 22 Nov 2019 10:19:51 GMT
Last-Modified: Fri, 15 Jan 2016 10:49:11 GMT
ETag: "37f89c297f08c822e21b634bf2c9ef2a-401"
Accept-Ranges: bytes
Content-Type: application/json
Content-Length: 3358565493
Server: AmazonS3
```

### Sample item

```json
{
  "asin": "0000031852",
  "title": "Girls Ballet Tutu Zebra Hot Pink",
  "price": 3.17,
  "imUrl": "http://ecx.images-amazon.com/images/I/51fAmVkTbyL._SY300_.jpg",
  "related":
  {
    "also_bought": ["B00JHONN1S", "B002BZX8Z6", "B00D2K1M3O", "0000031909", "B00613WDTQ", "B00D0WDS9A", "B00D0GCI8S", "0000031895", "B003AVKOP2", "B003AVEU6G", "B003IEDM9Q", "B002R0FA24", "B00D23MC6W", "B00D2K0PA0", "B00538F5OK", "B00CEV86I6", "B002R0FABA", "B00D10CLVW", "B003AVNY6I", "B002GZGI4E", "B001T9NUFS", "B002R0F7FE", "B00E1YRI4C", "B008UBQZKU", "B00D103F8U", "B007R2RM8W"],
    "also_viewed": ["B002BZX8Z6", "B00JHONN1S", "B008F0SU0Y", "B00D23MC6W", "B00AFDOPDA", "B00E1YRI4C", "B002GZGI4E", "B003AVKOP2", "B00D9C1WBM", "B00CEV8366", "B00CEUX0D8", "B0079ME3KU", "B00CEUWY8K", "B004FOEEHC", "0000031895", "B00BC4GY9Y", "B003XRKA7A", "B00K18LKX2", "B00EM7KAG6", "B00AMQ17JA", "B00D9C32NI", "B002C3Y6WG", "B00JLL4L5Y", "B003AVNY6I", "B008UBQZKU", "B00D0WDS9A", "B00613WDTQ", "B00538F5OK", "B005C4Y4F6", "B004LHZ1NY", "B00CPHX76U", "B00CEUWUZC", "B00IJVASUE", "B00GOR07RE", "B00J2GTM0W", "B00JHNSNSM", "B003IEDM9Q", "B00CYBU84G", "B008VV8NSQ", "B00CYBULSO", "B00I2UHSZA", "B005F50FXC", "B007LCQI3S", "B00DP68AVW", "B009RXWNSI", "B003AVEU6G", "B00HSOJB9M", "B00EHAGZNA", "B0046W9T8C", "B00E79VW6Q", "B00D10CLVW", "B00B0AVO54", "B00E95LC8Q", "B00GOR92SO", "B007ZN5Y56", "B00AL2569W", "B00B608000", "B008F0SMUC", "B00BFXLZ8M"],
    "bought_together": ["B002BZX8Z6"]
  },
  "salesRank": {"Toys & Games": 211836},
  "brand": "Coxlures",
  "categories": [["Sports & Outdoors", "Other Sports", "Dance"]]
}
```

where

```text
asin - ID of the product, e.g. 0000031852
title - name of the product
price - price in US dollars (at time of crawl)
imUrl - url of the product image
related - related products (also bought, also viewed, bought together, buy after viewing)
salesRank - sales rank information
brand - brand name
categories - list of categories the product belongs to
```

## Download files to a GCP compute engine

```shell
curl -o item_dedup.json.gz https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/item_dedup.json.gz
```

gave

```text
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100 17.8G  100 17.8G    0     0  63.9M      0  0:04:45  0:04:45 --:--:-- 62.1M
```

```shell
curl -o metadata.json.gz https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/metadata.json.gz
```

gave

```text
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100 3202M  100 3202M    0     0  63.7M      0  0:00:50  0:00:50 --:--:-- 62.6M
```

SELECT ls.LISTING_ID, ls.CREATED_AT, rev.LISTING_ID, rev.REVIEW_DATE, rev.REVIEW_SENTIMENT 
FROM {{ ref('dim_listings_cleansed') }} ls
RIGHT JOIN {{ ref('fct_reviews') }} rev 
ON ls.LISTING_ID = rev.LISTING_ID
WHERE ls.CREATED_AT > rev.REVIEW_DATE
{% snapshot scd_raw_listings %}

{{
    config(
        target_schema = 'dev_2',
        unique_key = 'id',
        strategy = 'timestamp',
        updated_at = 'updated_at',
        invalidate_hard_deletes = True
    )
}}

select * from {{ source('airbnb', 'listings') }} -- Which table to track

{% endsnapshot %}
{#
Hey there, a friendly comment here to let you know that BigQuery has billable limits. This model
is specifically very large, there are tons of blocks out there! If you're running into a bug
"500 Query exceeded limit for bytes billed: 1000000000000" then I recommend you head over to your profiles
setup. From here, you will need to increase the maximum_bytes_billed. If you are on the free tier you
should have enough credits to get this to startup. If you are billing yourself or off the trial credits
BE AWARE you could accidentally stack up an enormous bill. Have fun!
#}

{{
    config(
        materialized='incremental',
        unique_key='block_hash',
        partition_by={
          "field": "created_at_pt",
          "data_type": "timestamp",
          "granularity": "day"
        }
    )
}}

select * from {{ ref('prep__ethereum_blocks') }}

{#
  If you're testing around, consider using a where to a specific date. This should help with
  processing enormous amounts of data.
 #}
{# where date(created_at_pt) = "2022-03-28" #}

{% if is_incremental() %}

  where created_at_pt > (select max(created_at_pt) from {{ this }})

{% endif %}

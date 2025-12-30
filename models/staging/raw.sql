SELECT 
    *

FROM {{ source('fpl_raw', 'raw_element_summary') }}

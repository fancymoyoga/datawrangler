
provider_id
provider_status (active, termed)
last_update_date


function upsert_provider(existing_data, update_data)
(
    # named the returned dataframe and removed provider_id as join criteria, because records with same provider_id, but different status
    merged_df = existing_data.merge(update_data, 'outer')

    #sort the results descending
    sorted_df = merged_df.sort_values('last_update_date', descending)

    #remove duplicates, keep first, because it is sorted that way
    final_df = sorted_df.drop_duplicates('provider_id', keep='first')

    #Plus, Oops! forgot to return the results
    return final_df
)


# 1. Use how='outer' and omit the 'on' criteria to prevent _x and _y columns
merged_df = existing_data.merge(update_date_data, how='outer')

# 2. Your exact sorting and drop_duplicates statement now works perfectly!
sorted_df = (
    merged_df.sort_values(by='last_update_date', ascending=False)
    .drop_duplicates(subset=['provider_id'], keep='first')
)

    )

    # Then filter for the row_num == 1 indicating the most recent column
    return_df = sorted_df[sorted_df['row_num'] == 1].drop(columns=['row_num'])
)

    final_df = merged_df[
    merged.sort_values('last_update_date', ascending=False)
    .groupby('provider_id')
    .cumcount() == 0
]   .reset_index(drop=True)
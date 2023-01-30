
-- convert into hashed loyalty id
sha2(cast(a.party_account_no as varchar(50)), 256) = b.hashed_loyalty_id

-- Convert into inferred loyalty ID
a.inferred_customer_id = sha2(cast(b.enterprise_customer_ID as varchar(50)), 256)
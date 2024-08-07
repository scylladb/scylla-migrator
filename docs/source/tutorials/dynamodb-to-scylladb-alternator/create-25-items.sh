#!/usr/bin/env sh

generate_25_items() {
  local items=""
  for i in `seq 1 25`; do
    items="${items}"'{
      "PutRequest": {
        "Item": {
          "id": { "S": "'"$(uuidgen)"'" },
          "col1": { "S": "'"$(uuidgen)"'" },
          "col2": { "S": "'"$(uuidgen)"'" },
          "col3": { "S": "'"$(uuidgen)"'" },
          "col4": { "S": "'"$(uuidgen)"'" },
          "col5": { "S": "'"$(uuidgen)"'" }
        }
      }
    },'
  done
  echo "${items%,}" # remove trailing comma
}

aws \
  --endpoint-url http://localhost:8000 \
  dynamodb batch-write-item \
    --request-items '{
      "Example": ['"$(generate_25_items)"']
    }' > /dev/null

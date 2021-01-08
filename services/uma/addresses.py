import json
from flask import Response
from datetime import datetime
from google.cloud import bigquery

# Setup bigquery client
client = bigquery.Client()
TABLE_ID = "drumstick.addresses"

# Declare "/address" route
def address__route(request):
  """Returns appropriate handler for GET and POST request

  Args:
      request (flask_Request): request object

  Returns:
      Response: Flask response containing status code and/or data + mimetype
  """

  if request.method == "POST":
    # Collect JSON body
    req_data = request.get_json()

    # Insert address
    return insert_address(
      req_data["address"],
      req_data["label"],
      req_data["eoa"],
      req_data["live"]
    )
  else:
    # Else, if GET request, return all addresses
    return get_all_addresses()

def get_all_addresses():
  """Returns all addresses from addresses table

  Returns:
      JSON: Table data
  """

  query_job = client.query(
    """
    SELECT * FROM drumstick.addresses
    """
  )

  # Get result of query
  results = query_job.result()

  # Convert BQ RowIterator to JSON
  data = []
  for row in results:
    data.append({
      "address": row.address,
      "label": row.label,
      "eoa": row.eoa,
      "live": row.live,
      "created": str(row.created),
      "updated": str(row.updated)
    })

  return Response(json.dumps(data), status=200, mimetype='application/json')

def insert_address(address, label, eoa, live):
  """Inserts addresses to bigquery addresses table

  Args:
      address (string): EOA or EMP address
      label (string): address label
      eoa (boolean): true for EOA addresses, false for EMP
      live (boolean): address status

  Returns:
      Response: Flask status response
  """

  # Stream rows to bigquery table
  errors = client.insert_rows_json(TABLE_ID, [{
    "address": address,
    "label": label,
    "eoa": eoa,
    "live": live,
    "created": datetime.now(tz=None).__str__(),
    "updated": datetime.now(tz=None).__str__()
  }])

  # If no errors
  if errors == []:
    # Return success response
    return Response(status=200)
  else:
    # Else, return 400
    return Response(status=400)

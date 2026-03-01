import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator, PageNumberPaginator


BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"


def paginated_getter():
    page = 1
    while True:
        response = RESTClient(base_url=BASE_URL).get(f"?page={page}")
        data = response.json()
        if not data:
            break
        yield data
        page += 1


@dlt.resource(name="rides", write_disposition="replace")
def ny_taxi():
    for page_data in paginated_getter():
        yield page_data


def main():
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="ny_taxi_data",
    )

    load_info = pipeline.run(ny_taxi())
    print(load_info)


if __name__ == "__main__":
    main()

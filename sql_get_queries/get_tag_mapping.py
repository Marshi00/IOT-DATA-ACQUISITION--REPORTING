def get_tag_mapping(engine):
    try:
        if engine is not False:
            # Execute the query using the engine directly
            with engine.connect() as connection:
                result = connection.execute("SELECT * FROM dim_tag").fetchall()

            # Convert the result into a JSON-serializable format
            data = [dict(row) for row in result]

            return {"error": False, "msg": "Successfully retrieved data from DB", "data": data}
        else:
            return {"error": True, "msg": "Couldn't connect to DB"}
    except Exception as e:
        # Handle any exceptions that occurred during the query execution
        return {"error": True, "msg": "An error occurred during the query execution", "data": str(e)}

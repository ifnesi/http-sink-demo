import json
import queue
import logging

from flask import Flask, render_template, request


####################
# Global Variables #
####################
data_queue = queue.Queue()


##################
# Webapp (Flask) #
##################
app = Flask(
    __name__,
    static_folder="static",
    template_folder="templates",
)
app.config["SECRET_KEY"] = "4639b92d-4a39-40a9-9e6c-0e8c7b91f866"


#################
# Flask routing #
#################
@app.errorhandler(404)
def page_not_found(e):
    return "Page not found", 404


@app.route("/", methods=["GET"])
def root():
    return render_template(
        "main.html",
    )


@app.route("/api/webhook/<topic>", methods=["POST"])
def webhook(topic: str):
    try:
        data = request.get_json()
        logging.info(f"{data}")
        if not isinstance(data, list):
            data = [data]
        for d in data:
            if isinstance(d, dict):
                d = json.dumps(d)
            data_queue.put([topic, d])

    except Exception as e:
        logging.error(str(e))

    return "OK", 200


@app.route("/get_data_queue", methods=["GET"])
def get_data_queue():
    data = list()
    while not data_queue.empty():
        data.append(data_queue.get())
    return data, 200


########
# Main #
########
if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d [%(levelname)s]: %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Start http server
    app.run(
        host="0.0.0.0",
        port=8888,
        debug=True,
    )

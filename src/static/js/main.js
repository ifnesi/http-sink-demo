$(document).ready(function () {
    setInterval(function(){ 
        get_data_queue();
    }, 1500);
});

function get_data_queue() {
    $.ajax({
        type: "GET",
        url: "/get_data_queue",
        success: function(data) {
            var text = $("#data-queue").val();
            for (var i = 0; i < data.length; i++) {
                text = text + data[i] + "\r\n";
            }
            $("#data-queue").val(text);
            var textarea = document.getElementById("data-queue");
            textarea.scrollTop = textarea.scrollHeight;
        }
  });
}
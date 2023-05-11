const alertContainerId = "alert-container";

const App = {

    methods: {
        createAlert: function (msg, alertType = 'danger') {
            const alert = document.createElement("div");
            alert.classList.add("alert");

            alert.classList.add(`alert-${alertType}`);

            alert.classList.add("alert-dismissible");
            alert.classList.add("fade");
            alert.setAttribute("role", "alert");
            const alertMessageContainer = document.createElement("span");
            const alertDismissButton = document.createElement("button");
            alertDismissButton.type = "button";
            alertDismissButton.classList.add("btn-close");
            alertDismissButton.setAttribute("data-bs-dismiss", "alert");
            alertDismissButton.setAttribute("aria-label", "Close");
            alert.appendChild(alertMessageContainer);
            alert.appendChild(alertDismissButton);

            if (!alert.classList.contains("show")) {
                alert.classList.add("show");
            }

            alertMessageContainer.innerText = msg;
            document.querySelector('#' + alertContainerId).appendChild(alert);
            // close alert after 10 seconds
            setTimeout(() => {
                alert.classList.remove("show");
                setTimeout(() => {
                    alert.remove();
                }, 500);
            }, 10000);

        },
        upload: function () {
            // get file to upload from input with id 'sh-file'
            const file = document.querySelector('#sh-file').files[0];
            console.log(file);
            if (file === undefined) {
                this.createAlert('Please select a file to upload');
                return;
            }
            const formData = new FormData();
            formData.append('file', file);
            // disable upload button
            document.querySelector('#upload-btn').disabled = true;
            fetch('uploadShellScript', {
                method: 'POST',
                body: formData
            })
                .then(response => response.json())
                .then(data => {
                    console.log(data);
                    // append a div to body and write the data to it
                    const div = document.createElement("div");
                    div.innerText = data;
                    document.body.appendChild(div);

                })
                .catch(error => {
                    console.error(error);
                    this.createAlert(error.message);

                }
                ).finally(() => {
                    // enable upload button
                    document.querySelector('#upload-btn').disabled = false;
                });
        }
    },
    init: function () {
        if (document.querySelector('#' + alertContainerId) === null) {
            const alertContainer = document.createElement("div");
            alertContainer.id = alertContainerId;
            //append to top of body
            document.body.insertBefore(alertContainer, document.body.firstChild);
        }
    }
}
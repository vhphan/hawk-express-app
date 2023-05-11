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
            const file = document.querySelector('#csv-file').files[0];
            console.log(file);
            if (file === undefined) {
                this.createAlert('Please select a file to upload');
                return;
            }
            const formData = new FormData();
            formData.append('file', file);
            // disable upload button
            document.querySelector('#spinner').classList.remove('d-none');
            document.querySelector('#upload-btn').disabled = true;
            // show footer
            document.querySelector('footer').classList.remove('d-none');
            this.fadeIn(document.querySelector('footer'));
            fetch('uploadCsvFile', {
                method: 'POST',
                body: formData
            })
                .then(response => response.json())
                .then(jsonData => {


                    console.log(jsonData);
                    const data = jsonData.data;

                    if (!jsonData.success) {
                        let errorMessage = jsonData.message || 'Something went wrong';
                        errorMessage += '. Please report this error to the developer vee.huen.phan@ericsson.com';
                        this.createAlert(errorMessage);

                        return;
                    };

                    // append a div to body and write the data to it
                    const div = document.createElement("div");

                    const { downloadLink, csvFileDownloadLink, zipFileName } = data;
                    if (downloadLink) {
                        const a = document.createElement("a");
                        a.href = downloadLink;
                        a.innerText = "Download CTR Files Zipped";
                        div.appendChild(a);
                        div.appendChild(document.createElement("br"));
                    }
                    if (csvFileDownloadLink) {
                        const a = document.createElement("a");
                        a.href = csvFileDownloadLink;
                        a.innerText = "Download CSV File of Summary of Success and Failures";
                        div.appendChild(a);
                    }
                    const p = document.createElement("p");
                    p.innerHTML = 'Number of Files (Success): ' + data.numberOfFilesZipped + '<br>';
                    p.innerHTML += 'Number of Files Not Found (Failed): ' + data.numberOfFilesNotFound + '<br>';
                    p.innerHTML += 'Number of Files Other Reasons (Failed): ' + data.numberOfFilesFailedToZip + '<br>';
                    p.innerHTML += `${zipFileName} is being uploaded to Sharepoint CTR Folder. Please check in a while!`

                    div.appendChild(p);


                    // div.innerText = data;
                    // append div to main
                    if (document.querySelector('#result') === null) {
                        const resultsContainer = document.createElement("div");
                        resultsContainer.id = 'results';
                        // add these classes row border-primary border p-2 m-2
                        resultsContainer.classList.add('row');
                        resultsContainer.classList.add('border-primary');
                        resultsContainer.classList.add('border');
                        resultsContainer.classList.add('p-2');
                        resultsContainer.classList.add('m-2');
                        document.body.appendChild(resultsContainer);
                    }
                    // empty results container
                    document.querySelector('#result').innerHTML = '';
                    document.querySelector('#result').appendChild(div);


                })
                .catch(error => {
                    console.error(error);
                    this.createAlert(error.message || 'Something went wrong');

                }
                ).finally(() => {
                    // enable upload button
                    document.querySelector('#upload-btn').disabled = false;
                    document.querySelector('#spinner').classList.add('d-none');
                    // hide footer
                    // document.querySelector('footer').classList.add('d-none');
                    this.fadeOut(document.querySelector('footer'));
                });
        },
        fadeIn: function (element, duration = 500) {
            element.style.display = '';
            element.style.opacity = 0;
            var last = +new Date();
            var tick = function () {
                element.style.opacity = +element.style.opacity + (new Date() - last) / duration;
                last = +new Date();
                if (+element.style.opacity < 1) {
                    (window.requestAnimationFrame && requestAnimationFrame(tick)) || setTimeout(tick, 16);
                }
            };
            tick();
        },

        fadeOut: function (element, duration = 8000) {
            element.style.opacity = 1;
            var last = +new Date();
            var tick = function () {
                element.style.opacity = +element.style.opacity - (new Date() - last) / duration;
                last = +new Date();
                if (+element.style.opacity > 0) {
                    (window.requestAnimationFrame && requestAnimationFrame(tick)) || setTimeout(tick, 16);
                } else {
                    element.style.display = 'none';
                }
            };
            tick();
        }

    },
    init: function () {
        if (document.querySelector('#' + alertContainerId) === null) {
            const alertContainer = document.createElement("div");
            alertContainer.id = alertContainerId;
            //append to top of body
            document.body.insertBefore(alertContainer, document.body.firstChild);
        }


        console.log('App initialized');
    }
}
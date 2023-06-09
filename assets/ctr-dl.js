
const createAlert = function (msg, alertType = 'danger') {
    const alert = document.createElement("div");
    alert.classList.add("alert");

    alert.classList.add(`alert-${alertType}`);

    alert.classList.add("alert-dismissible");
    alert.classList.add("fade");
    alert.setAttribute("role", "alert");
    // alert.innerHTML = `
    //   <span id="my-alert-message"></span>
    //   <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    // `;
    const alertMessageContainer = document.createElement("span");
    const alertDismissButton = document.createElement("button");
    alertDismissButton.type = "button";
    alertDismissButton.classList.add("btn-close");
    alertDismissButton.setAttribute("data-bs-dismiss", "alert");
    alertDismissButton.setAttribute("aria-label", "Close");
    alert.appendChild(alertMessageContainer);
    alert.appendChild(alertDismissButton);

    // show alert: add show class if not in class list
    if (!alert.classList.contains("show")) {
        alert.classList.add("show");
    }

    alertMessageContainer.innerText = msg;
    document.querySelector("#alert-container").appendChild(alert);
    // close alert after 10 seconds
    setTimeout(() => {
        alert.classList.remove("show");
        setTimeout(() => {
            alert.remove();
        }, 500);
    }, 10000);

};

const myFetchJson = async function (url, options) {
    const response = await window.fetch(url, options);
    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.message);
    }
    const jsonResponse = await response.json();
    if (!jsonResponse.success) {
        createAlert(jsonResponse.message);
    }
    if (jsonResponse.data === null || jsonResponse.data.length === 0) {
        createAlert("No data returned from server.");
    }
    if (jsonResponse.message && jsonResponse.message.length > 0 && jsonResponse.success) {
        createAlert(jsonResponse.message), 'primary';
    }

    return jsonResponse;
};


// functions to retrieve data from server and populate the select box
const getAvailableTimestamps = async function () {
    const availableTimestamps = await (
        await fetch("getAvailableTimestamps")
    ).json();
    if (availableTimestamps.data.length === 0) {
        createAlert("No timestamps available. Please try again later.");
        return;
    }

    const select = document.getElementById("select-timestamp");

    // remove all current options
    while (select.firstChild) {
        select.removeChild(select.firstChild);
    }

    availableTimestamps.data.forEach((timestamp) => {
        const option = document.createElement("option");
        option.value = timestamp;
        option.innerText = timestamp;
        select.appendChild(option);
    });
};

const getAvailableSites = async function () {
    const siteSubstring = document.getElementById("site-name").value;
    const selectedDate = document.getElementById("selected-date").value;
    const selectedHour = document.getElementById("selected-hour").value;
    const selectedHourEnd = document.getElementById("selected-hour-end").value;

    // make sure there are at least 3 characters
    if (siteSubstring.length < 5) {
        createAlert(
            "Please enter at least 5 characters to search to search sites."
        );
        return;
    }

    const availableSites = await (
        await fetch(
            `getAvailableSites?selectedDate=${selectedDate}&selectedHour=${selectedHour}&siteSubstring=${siteSubstring}`
        )
    ).json();

    if (availableSites.data.length === 0) {
        createAlert(
            "No sites available. Please try different search criterias."
        );
        return;
    }

    const select = document.getElementById("sites");

    // remove all current options
    while (select.firstChild) {
        select.removeChild(select.firstChild);
    }

    availableSites.data.forEach((site) => {
        const option = document.createElement("option");
        option.value = site;
        option.innerText = site;
        select.appendChild(option);
    });
};

const loadScript = function (url, scriptClass, callback = null) {
    let isLoaded = document.querySelectorAll("." + scriptClass);
    if (!(isLoaded.length > 0)) {
        let myScript = document.createElement("script");
        myScript.src = url;
        myScript.className = scriptClass;
        document.body.appendChild(myScript);
        myScript.onload = function () {
            if (callback) callback();
        };
    }
};

const loadCss = function (url, cssClass, callback = null) {
    let isLoaded = document.querySelectorAll("." + cssClass);
    if (!(isLoaded.length > 0)) {
        let myCss = document.createElement("link");
        myCss.href = url;
        myCss.rel = "stylesheet";
        myCss.className = cssClass;
        document.head.appendChild(myCss);
        myCss.onload = function () {
            if (callback) callback();
        };
    }
};

const addSitesToList = function () {
    const sites = document.getElementById("sites");

    const selectedSites = document.getElementById("selected-sites");
    const selectedSitesOptions = selectedSites.options;

    const sitesOptions = sites.options;
    const selectedSitesOptionsLength = selectedSitesOptions.length;
    const sitesOptionsLength = sitesOptions.length;

    for (let i = 0; i < sitesOptionsLength; i++) {
        if (sitesOptions[i].selected) {
            let option = document.createElement("option");
            option.value = sitesOptions[i].value;
            option.text = sitesOptions[i].text;
            // check is selected site is already in the list
            let isDuplicate = false;
            for (let j = 0; j < selectedSitesOptionsLength; j++) {
                if (selectedSitesOptions[j].value === option.value) {
                    isDuplicate = true;
                    break;
                }
            }
            if (!isDuplicate) {
                selectedSitesOptions.add(option);
            }
        }
    }
    // alert if more than 3 sites are selected
    if (selectedSitesOptions.length > 3) {
        createAlert(
            "You have selected more than the limit of 3 sites. Only the first 3 sites will be used."
        );
    }
    // remove all but first 3 options
    while (selectedSitesOptions.length > 3) {
        selectedSitesOptions.remove(3);
    }

};

const removeSitesFromList = function () {
    const selectedSites = document.getElementById("selected-sites");
    const selectedSitesOptions = selectedSites.options;
    const selectedSitesOptionsLength = selectedSitesOptions.length;
    indexOfSitesToRemove = [];
    for (let i = 0; i < selectedSitesOptionsLength; i++) {
        if (selectedSitesOptions[i].selected) {
            indexOfSitesToRemove.push(i);
        }
    }
    indexOfSitesToRemove.reverse().forEach((index) => {
        selectedSitesOptions.remove(index);
    });
};

const filterSitesList = function () {
    const filterString = document.getElementById("filter-sites").value;
    const sites = document.getElementById("sites");
    const sitesOptions = sites.options;
    const sitesOptionsLength = sitesOptions.length;
    for (let i = 0; i < sitesOptionsLength; i++) {
        if (
            sitesOptions[i].text
                .toLowerCase()
                .includes(filterString.toLowerCase())
        ) {
            sitesOptions[i].style.display = "block";
        } else {
            sitesOptions[i].style.display = "none";
        }
    }
};

const filterSelectedSitesList = function () {
    const filterString = document.getElementById(
        "filter-selected-sites"
    ).value;
    const selectedSites = document.getElementById("selected-sites");
    const selectedSitesOptions = selectedSites.options;
    const selectedSitesOptionsLength = selectedSitesOptions.length;
    for (let i = 0; i < selectedSitesOptionsLength; i++) {
        if (
            selectedSitesOptions[i].text
                .toLowerCase()
                .includes(filterString.toLowerCase())
        ) {
            selectedSitesOptions[i].style.display = "block";
        } else {
            selectedSitesOptions[i].style.display = "none";
        }
    }
};

const downloadSelectedSites = async function () {

    const selectedDate =
        document.querySelector("#selected-date").value;

    const selectedHour =
        document.querySelector("#selected-hour").value;

    const selectedHourEnd = document.querySelector("#selected-hour-end").value;

    const selectedSitesValues = Array.from(
        document.querySelectorAll("#selected-sites option")
    ).map((option) => option.value);

    selectedSitesQueryParams = selectedSitesValues
        .map((d) => `sites[]=${d}`)
        .join("&");

    const spinnerSpan = document.querySelector('#spinner-span');
    const downloadDiv = document.getElementById("download-link");

    // disable the download button
    disableButtons();
    // add a spinner to download div
    spinnerSpan.innerHTML = `
    <p>Preparing download link... Please wait...</p>
    <div class="spinner-grow text-primary" style="width: 5rem; height: 5rem;" role="status">
        <span class="visually-hidden">Loading...</span>
    </div>

    `;
    downloadDiv.innerHTML = "";

    // additional parameters to be passed to the fetch function
    // file types checkboxes
    const fileTypes = document.querySelectorAll('.file-type-to-download');
    const fileTypesToDownload = Array.from(fileTypes).filter((fileType) => fileType.checked).map((fileType) => fileType.value);
    const fileTypesToDownloadQueryParams = fileTypesToDownload.map((d) => `fileTypes[]=${d}`).join("&");


    try {
        const result = await myFetchJson(
            `downloadSelectedSites?selectedDate=${selectedDate}&selectedHour=${selectedHour}&selectedHourEnd=${selectedHourEnd}&${selectedSitesQueryParams}&${fileTypesToDownloadQueryParams}`
        );
        const { data } = result;
        const { downloadLink, zipFilename } = data;
        createAlert("Download link is ready. Please click the downlink to start downloading.", 'primary');
        // get the base url
        const baseUrl = window.location.origin;
        //get all url path components from current location except the last one
        const urlPathComponents = window.location.pathname.split('/').slice(0, -1);
        //join the base url and the url path components
        const urlPath = urlPathComponents.join('/');
        //create the full url
        const fullUrl = baseUrl + urlPath;


        //add the download link to the download div
        downloadDiv.innerHTML = `

        <p>
        Download is ready. Please click the link below to download the file.<br/>
            <strong>Download link:</strong>
        </p>
        <a style="font-size:2rem;" href="${fullUrl}/${downloadLink}" target="_blank">Download</a>

        `;

    } catch (error) {
        console.log(error);
        createAlert("Error while downloading the file. Please try again.");
        downloadDiv.innerHTML = "";
    } finally {
        spinnerSpan.innerHTML = "";
        enableButtons();
    }

};

function resetForm() {
    document.getElementById("sites").innerHTML = "";
    // clear the selected sites list
    document.getElementById("selected-sites").innerHTML = "";
    // clear the download link
    document.getElementById("download-link").innerHTML = "";
}

function disableButtons() {
    document.getElementById("search-site").disabled = true;
    document.getElementById("download-selected-sites").disabled = true;
}

function enableButtons() {
    document.getElementById("search-site").disabled = false;
    document.getElementById("download-selected-sites").disabled = false;
}


const App = {

    init: function () {

        // events

        document
            .getElementById("search-site")
            .addEventListener("click", async () => {

                // check if the date and hour are selected
                const selectedDate =
                    document.querySelector("#selected-date").value;

                const selectedHour =
                    document.querySelector("#selected-hour").value;

                if (selectedDate === "" || selectedHour === "") {
                    createAlert("Please select a date and hour.");
                    return;
                }

                const spinnerDiv = document.querySelector('#spinner-span-search-sites');
                spinnerDiv.innerHTML = `
                <p>Fetching data... Please wait...</p>
                <div class="spinner-grow text-primary" style="width: 5rem; height: 5rem;" role="status">
                    <span class="visually-hidden">Loading...</span>
                </div>
                `;
                try {
                    resetForm();
                    // disable all buttons
                    disableButtons();
                    await getAvailableSites();
                } catch (error) {
                    console.log(error);
                } finally {
                    spinnerDiv.innerHTML = "";
                    enableButtons();
                }

            });

        document.getElementById("add-site").addEventListener("click", () => {
            addSitesToList();
        });

        document
            .getElementById("remove-site")
            .addEventListener("click", () => {
                removeSitesFromList();
            });

        document
            .getElementById("filter-sites")
            .addEventListener("keyup", () => {
                filterSitesList();
            });

        document
            .getElementById("filter-selected-sites")
            .addEventListener("keyup", () => {
                filterSelectedSitesList();
            });

        document
            .getElementById("download-selected-sites")
            .addEventListener("click", () => {
                downloadSelectedSites();
            });

        document
            .querySelector("#selected-date")
            .addEventListener("change", () => {
                resetForm();
            });

        const isSelectedHoursValid = () => {
            const selectedHourEnd = parseInt(document.querySelector("#selected-hour-end").value);
            const selectedHour = parseInt(document.querySelector("#selected-hour").value);

            // check if the end hour is selected. if not set to the start hour
            if (selectedHourEnd === 0) {
                document.querySelector("#selected-hour-end").value = document.querySelector("#selected-hour").value;
                return true;
            }

            if (selectedHourEnd < selectedHour) {
                createAlert("Invalid hour range. End hour is set to Start hour.");
                document.querySelector("#selected-hour-end").value = document.querySelector("#selected-hour").value;;
                return false;
            }
            return true;
        }

        document
            .querySelector("#selected-hour")
            .addEventListener("change", () => {
                if (!isSelectedHoursValid()) {
                    return;
                }
                resetForm();
            });

        document
            .querySelector("#selected-hour-end")
            .addEventListener("change", () => {
                if (!isSelectedHoursValid()) {
                    return;
                }
                resetForm();
            });


        document.querySelectorAll('.file-type-to-download').forEach((fileType) => {
            fileType.addEventListener('change', () => {
                // clear the download link
                document.getElementById("download-link").innerHTML = "";
            })
        });



    }

}


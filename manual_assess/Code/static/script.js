document.getElementById('query').addEventListener('keypress', function(event) {
    if (event.key === 'Enter') {
        let autocompleteResults = document.getElementById('autocomplete-results');
        autocompleteResults.innerHTML = '';
        autocompleteResults.style.display = 'none';
        search();
        event.preventDefault();
    }
});

document.getElementById('query').addEventListener('input', function(event) {
    const query = event.target.value;
    if (query.length > 2) {
        fetch(`/autocomplete?query=${encodeURIComponent(query)}`)
            .then(response => response.json())
            .then(suggestions => {
                let autocompleteResults = document.getElementById('autocomplete-results');
                let resultsDiv = document.getElementById('results');
                autocompleteResults.innerHTML = '';

                if (suggestions.length > 0 && resultsDiv.innerHTML == '') {
                    autocompleteResults.style.display = 'block';
                } else {
                    autocompleteResults.style.display = 'none';
                }
                suggestions.forEach((suggestion) => {
                    let div = document.createElement('div');
                    div.innerHTML = `<a href="${ suggestion.url }">${ suggestion.title }  <br>  ${suggestion.url}</a>`;
                    div.onclick = function() {
                        document.getElementById('query').value = suggestion;
                        autocompleteResults.innerHTML = '';
                    };
                    autocompleteResults.appendChild(div);
                });
            })
            .catch(error => console.error('Error:', error));
    }
});

document.addEventListener('click', function(event) {
    let autocompleteResults = document.getElementById('autocomplete-results');
    if (event.target.id !== 'query') {
        autocompleteResults.style.display = 'none';
    }
});

function search() {
    var query = document.getElementById('query').value;
    fetch(`/search?query=${encodeURIComponent(query)}`)
        .then(response => response.json())
        .then(data => {
            let resultsDiv = document.getElementById('results');
            resultsDiv.innerHTML = '<form id="assessment-form">';
            data.forEach((hit, index) => {
                let div = document.createElement('div');
                div.className = 'result-item';
                div.innerHTML = `
                    <p><b>Title:</b> ${hit.title}<br><b>Snippet:</b> ${hit.snippet}<br><a href="${hit.url}"><b>Url:</b> ${hit.url}</a></p>
                    <div>
                        <label><input type="radio" name="grade-${index}" value="0"> Non-Relevant</label>
                        <label><input type="radio" name="grade-${index}" value="1"> Relevant</label>
                        <label><input type="radio" name="grade-${index}" value="2"> Very Relevant</label>
                    </div>
                `;
                resultsDiv.appendChild(div);
            });
            // resultsDiv.innerHTML += '<button type="button" onclick="submitAssessments()">Submit Assessments</button></form>';
        })
        .catch(error => console.error('Error:', error));
}

function submitAssessments() {
    const assessorId = document.getElementById('assessor-id').value;
    if (!assessorId) {
        console.error('Assessor ID is required');
        alert('Please enter your Assessor ID.');
        return;
    }
    const assessments = Array.from(document.querySelectorAll('.result-item')).map((item, index) => {
        const url = item.querySelector('a').href;
        const gradeInputs = document.getElementsByName(`grade-${index}`);
        const grade = Array.from(gradeInputs).find(input => input.checked)?.value;
        if (!grade) {
            console.error(`No grade selected for URL: ${url}`);
        }
        return { query_id:150902, url, grade };
    }).filter(assessment => assessment.grade !== undefined);
    console.log('Submitting the following assessments:', assessments);

    fetch('/assess', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            assessor_id: assessorId,
            assessments: assessments
        })
    })
    .then(response => {
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }
        return response.json();
    })
    .then(data => {
        console.log('Success:', data.message);
        alert('Assessments submitted successfully!');
    })
    .catch(error => {
        console.error('Error during submission:', error);
        alert('Failed to submit assessments. Please try again.');
    });
}



var currentPage = 0;

function search(page = 0) {
    var query = document.getElementById('query').value;
    currentPage = page; // Update current page
    fetch(`/search?query=${encodeURIComponent(query)}&page=${page}`)
        .then(response => response.json())
        .then(data => {
            let resultsDiv = document.getElementById('results');
            resultsDiv.innerHTML = '<form id="assessment-form">';
            data.forEach((hit, index) => {
                let div = document.createElement('div');
                div.className = 'result-item';
                div.innerHTML = `
                    <p><b>Title:</b> ${hit.title}<br><b>Snippet:</b> ${hit.snippet}<br><a href="${hit.url}"><b>Url:</b> ${hit.url}</a></p>
                    <div>
                        <label><input type="radio" name="grade-${index}" value="0"> Non-Relevant</label>
                        <label><input type="radio" name="grade-${index}" value="1"> Relevant</label>
                        <label><input type="radio" name="grade-${index}" value="2"> Very Relevant</label>
                    </div>
                `;
                resultsDiv.appendChild(div);
            });
        })
        .catch(error => console.error('Error:', error));
}

function nextPage() {
    search(currentPage + 1);
}

function prevPage() {
    if (currentPage > 0) {
        search(currentPage - 1);
    }
}





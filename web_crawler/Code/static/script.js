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
            resultsDiv.innerHTML = '';
            data.forEach(hit => {
                let div = document.createElement('div');
                div.innerHTML = `<p> <b>Title:</b> ${ hit.title } <br> <B>Snippet:</b> ${hit.snippet} <br> <a href="${ hit.url }"> <b>Url:</b> ${hit.url}</a> <br> <p>`;
                resultsDiv.appendChild(div);
            });
        })
        .catch(error => console.error('Error:', error));
}
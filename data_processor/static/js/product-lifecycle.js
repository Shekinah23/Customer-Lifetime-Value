function updateLifecycleStage(productCode) {
    if (!productCode) {
        // If no product selected, refresh page to show all products combined
        window.location.reload();
        return;
    }

    fetch(`/api/product/lifecycle/${productCode}`)
        .then(response => response.json())
        .then(stages => {
            // Update the progress bar
            const stageNames = ['Introduction', 'Growth', 'Maturity', 'Decline'];
            stageNames.forEach(stageName => {
                const stageContainer = document.querySelector(`[data-stage="${stageName}"]`).parentElement;
                const currentStage = stages.find(s => s.name === stageName && s.current);
                if (stageContainer) {
                    const progressBar = stageContainer.querySelector(`[data-stage="${stageName}"]`);
                    progressBar.classList.toggle('opacity-30', !currentStage);
                    progressBar.classList.toggle('opacity-100', !!currentStage);
                    
                    // Update description box
                    const descriptionBox = stageContainer.querySelector('.bg-white');
                    if (descriptionBox) {
                        let description = '';
                        switch(stageName) {
                            case 'Introduction':
                                description = 'New products gaining market traction';
                                break;
                            case 'Growth':
                                description = 'Rapidly increasing customer adoption and revenue';
                                break;
                            case 'Maturity':
                                description = 'Stable market share and consistent revenue';
                                break;
                            case 'Decline':
                                description = 'Decreasing market share and revenue';
                                break;
                        }
                        descriptionBox.textContent = description;
                    }
                }
            });

            // Update stage details
            const stageDetails = document.querySelector('.lifecycle-stages');
            if (stageDetails) {
                stageDetails.innerHTML = stages.map(stage => `
                    <div class="p-4 rounded-lg ${stage.current ? 'bg-blue-50 border-2 border-blue-200' : 'bg-gray-50'}">
                        <div class="flex justify-between items-center">
                            <div>
                                <h4 class="font-semibold text-gray-900">${stage.name}</h4>
                                <p class="text-sm text-gray-600">${stage.description}</p>
                            </div>
                            ${stage.current ? '<span class="px-3 py-1 text-xs font-semibold rounded-full bg-blue-100 text-blue-800">Current</span>' : ''}
                        </div>
                    </div>
                `).join('');
            }
        })
        .catch(error => console.error('Error updating lifecycle stage:', error));
}

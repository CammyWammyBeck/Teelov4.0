// Match detail page — collapsible feature group sections

document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('.feature-group-toggle').forEach(btn => {
        btn.addEventListener('click', () => {
            const targetId = btn.dataset.target;
            const content = document.getElementById(targetId);
            if (!content) return;

            const isExpanded = btn.getAttribute('aria-expanded') === 'true';
            btn.setAttribute('aria-expanded', !isExpanded);
            content.classList.toggle('hidden');

            // Rotate chevron icon
            const icon = btn.querySelector('[data-lucide]');
            if (icon) {
                icon.classList.toggle('rotate-[-90deg]', isExpanded);
            }
        });
    });

    // Re-init Lucide icons for any dynamic content
    if (typeof lucide !== 'undefined') {
        lucide.createIcons();
    }
});

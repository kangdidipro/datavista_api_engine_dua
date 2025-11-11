export default () => ({
    sidebarOpen: true,
    activeAccordion: null,
    activeFlyout: null,

    toggleSidebar() {
        this.sidebarOpen = !this.sidebarOpen;
        this.activeAccordion = null; // Close accordions when sidebar state changes
        this.activeFlyout = null; // Close flyouts when sidebar state changes
    },

    toggleAccordion(index) {
        if (this.activeAccordion === index) {
            this.activeAccordion = null;
        } else {
            this.activeAccordion = index;
        }
    },

    toggleFlyout(index) {
        if (this.activeFlyout === index) {
            this.activeFlyout = null;
        } else {
            this.activeFlyout = index;
        }
    },

    handleMenuClick(index) {
        if (this.sidebarOpen) {
            this.toggleAccordion(index);
        } else {
            this.toggleFlyout(index);
        }
    },

    closeFlyout() {
        this.activeFlyout = null;
    }
});

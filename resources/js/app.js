import './bootstrap';

import Alpine from 'alpinejs';
import sidebar from './sidebar';

window.Alpine = Alpine;

Alpine.data('sidebar', sidebar);

Alpine.start();

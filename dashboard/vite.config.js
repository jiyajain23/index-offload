import {defineConfig} from 'vite';
export default defineConfig({base:'./',server:{proxy:{'/api':'http://localhost:8000','/health':'http://localhost:8000'}}});

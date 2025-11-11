<?php

namespace App\Providers;

use Illuminate\Support\ServiceProvider;
use Illuminate\Support\Facades\View;
use Illuminate\Support\Facades\File;

class AppServiceProvider extends ServiceProvider
{
    /**
     * Register any application services.
     */
    public function register(): void
    {
        //
    }

    /**
     * Bootstrap any application services.
     */
    public function boot(): void
    {
        View::composer('*', function ($view) {
            $path = base_path('../struktur_menu.json');
            $menuStructure = [];
            if (File::exists($path)) {
                $menuStructure = json_decode(File::get($path), true);
            }
            
            $currentRoute = '/' . request()->path();
            $pageDescription = '';

            foreach ($menuStructure as $menu) {
                if (!empty($menu['sub_menu'])) {
                    foreach ($menu['sub_menu'] as $submenu) {
                        if (isset($submenu['route']) && $submenu['route'] === $currentRoute) {
                            $pageDescription = $submenu['deskripsi'];
                            break 2;
                        }
                        if (!empty($submenu['sub_menu'])) {
                            foreach ($submenu['sub_menu'] as $subsubmenu) {
                                if (isset($subsubmenu['route']) && $subsubmenu['route'] === $currentRoute) {
                                    $pageDescription = $subsubmenu['deskripsi'];
                                    break 3;
                                }
                            }
                        }
                    }
                }
            }

            $view->with('menuStructure', $menuStructure)
                 ->with('pageDescription', $pageDescription);

            $helpPath = base_path('../bantuan_help.json');
            $bantuanHelp = [];
            if (File::exists($helpPath)) {
                $bantuanHelp = json_decode(File::get($helpPath), true);
            }
            $view->with('bantuanHelp', $bantuanHelp);
        });
    }
}

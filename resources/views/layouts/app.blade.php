<!DOCTYPE html>
<html lang="{{ str_replace('_', '-', app()->getLocale()) }}">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="csrf-token" content="{{ csrf_token() }}">
    <title>{{ config('app.name', 'Laravel') }}</title>
    <link rel="icon" href="{{ asset('images/favicon.ico') }}" type="image/x-icon">
    <link rel="preconnect" href="https://fonts.bunny.net">
    <link href="https://fonts.bunny.net/css?family=figtree:400,500,600&display=swap" rel="stylesheet" />
    @vite(['resources/css/app.css', 'resources/js/app.js'])
    <style>
        .menu-icon svg { width: 100%; height: 100%; }
    </style>
</head>
<body class="font-sans antialiased">
    @php
    $menuItems = $menuStructure ?? [];
    @endphp
    <div x-data="{ sidebarOpen: true }" class="min-h-screen bg-gray-100">
        <aside
            class="fixed inset-y-0 left-0 z-30 bg-white shadow-lg transform transition-all duration-300 ease-in-out flex flex-col"
            :class="sidebarOpen ? 'w-64' : 'w-20'"
        >
            <div class="flex items-center justify-center h-16 border-b flex-shrink-0">
                <a href="{{ route('dashboard') }}">
                    <img src="{{ asset('images/logo.png') }}" alt="Logo" class="block h-10 w-auto" />
                </a>
            </div>
            <nav class="mt-4 flex-grow overflow-y-auto pb-4">
                @foreach ($menuItems as $menu)
                    <div class="px-2 mb-2">
                        @if (!empty($menu['sub_menu']))
                            <div x-data="{ open: false }" class="relative" @click.away="if (!sidebarOpen) open = false">
                                <button @click="open = !open" class="w-full flex items-center justify-between p-2 text-sm font-medium text-gray-600 rounded-md hover:bg-gray-100 focus:outline-none">
                                    <div class="flex items-center">
                                        <span class="w-6 h-6 flex-shrink-0 menu-icon">{!! $menu['icon'] !!}</span>
                                        <span class="ml-3 whitespace-nowrap" x-show="sidebarOpen">{{ $menu['nama_menu'] }}</span>
                                    </div>
                                    <svg x-show="sidebarOpen" class="w-4 h-4 transition-transform duration-200" :class="{'rotate-180': open}" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>
                                </button>

                                <!-- Accordion for WIDE sidebar -->
                                <div x-show="open && sidebarOpen" x-transition class="mt-1 ml-4 pl-4 border-l border-gray-200">
                                    @foreach ($menu['sub_menu'] as $submenu)
                                        @if (!empty($submenu['sub_menu']))
                                            <div class="mt-2">
                                                <div class="block px-2 py-1 text-xs font-semibold text-gray-400">{{ $submenu['nama'] }}</div>
                                                <div class="ps-2">
                                                    @foreach ($submenu['sub_menu'] as $subsubmenu)
                                                        <a href="{{ url($subsubmenu['route']) }}" @class(['block p-2 text-sm rounded-md', 'bg-blue-100 text-blue-700' => request()->is(ltrim($subsubmenu['route'], '/')), 'text-gray-600 hover:bg-gray-100' => !request()->is(ltrim($subsubmenu['route'], '/'))])>
                                                            {{ $subsubmenu['nama'] }}
                                                        </a>
                                                    @endforeach
                                                </div>
                                            </div>
                                        @else
                                            <a href="{{ url($submenu['route']) }}" @class(['block p-2 text-sm rounded-md', 'bg-blue-100 text-blue-700' => request()->is(ltrim($submenu['route'], '/')), 'text-gray-600 hover:bg-gray-100' => !request()->is(ltrim($submenu['route'], '/'))])>
                                                {{ $submenu['nama'] }}
                                            </a>
                                        @endif
                                    @endforeach
                                </div>

                                <!-- Fly-out for COLLAPSED sidebar -->
                                <div x-show="open && !sidebarOpen" @click.away="open = false" class="absolute left-full top-0 ml-2 z-40 w-60 bg-white shadow-lg rounded-md p-2" x-transition>
                                    <div class="font-bold text-sm text-gray-800 p-2">{{ $menu['nama_menu'] }}</div>
                                    @foreach ($menu['sub_menu'] as $submenu)
                                        @if (!empty($submenu['sub_menu']))
                                            <div class="border-t border-gray-200 mt-1 pt-1"></div>
                                            <div class="block px-2 py-1 text-xs text-gray-400">{{ $submenu['nama'] }}</div>
                                            @foreach ($submenu['sub_menu'] as $subsubmenu)
                                                <a href="{{ url($subsubmenu['route']) }}" class="block ps-4 p-2 text-sm text-gray-600 rounded-md hover:bg-gray-100">{{ $subsubmenu['nama'] }}</a>
                                            @endforeach
                                        @else
                                            <a href="{{ url($submenu['route']) }}" class="block p-2 text-sm text-gray-600 rounded-md hover:bg-gray-100">{{ $submenu['nama'] }}</a>
                                        @endif
                                    @endforeach
                                </div>
                            </div>
                        @else
                            <a href="{{ url($menu['route']) }}" @class(['flex items-center p-2 text-sm font-medium rounded-md', 'bg-blue-100 text-blue-700' => request()->is(ltrim($menu['route'], '/')), 'text-gray-600 hover:bg-gray-100' => !request()->is(ltrim($menu['route'], '/'))])>
                                <span class="w-6 h-6 flex-shrink-0 menu-icon">{!! $menu['icon'] !!}</span>
                                <span class="ml-3 whitespace-nowrap" x-show="sidebarOpen">{{ $menu['nama_menu'] }}</span>
                            </a>
                        @endif
                    </div>
                @endforeach
            </nav>
        </aside>

        <div class="flex flex-col flex-1 transition-all duration-300 ease-in-out" :class="sidebarOpen ? 'sm:ml-64' : 'sm:ml-20'">
            @include('layouts.navigation')
            @if (isset($header))
                <header class="bg-white shadow"><div class="max-w-7xl mx-auto py-6 px-4 sm:px-6 lg:px-8">{{ $header }}</div></header>
            @endif
            <main>{{ $slot }}</main>
        </div>
    </div>
</body>
</html>
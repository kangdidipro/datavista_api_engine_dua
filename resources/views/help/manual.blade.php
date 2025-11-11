<x-app-layout>
    <x-slot name="header">
        <h2 class="font-semibold text-xl text-gray-800 leading-tight">
            {{ __('Panduan Pengguna') }}
        </h2>
    </x-slot>

    <div class="py-12">
        <div class="max-w-7xl mx-auto sm:px-6 lg:px-8">
            <div class="bg-white overflow-hidden shadow-sm sm:rounded-lg">
                @if(!empty($bantuanHelp['bantuan_help'][2]['konten']))
                    <div class="p-6 bg-white border-b border-gray-200 space-y-8">
                        @foreach($bantuanHelp['bantuan_help'][2]['konten'] as $section)
                            <div>
                                <h3 class="text-lg font-semibold text-gray-900">{{ $section['judul'] }}</h3>
                                @if(isset($section['deskripsi']))
                                    <p class="mt-1 text-gray-600">{{ $section['deskripsi'] }}</p>
                                @endif
                                <div class="mt-4 space-y-4">
                                    @foreach($section['langkah'] as $step)
                                        <div class="p-4 bg-gray-50 rounded-lg border-l-4 border-blue-500">
                                            <h4 class="font-bold text-gray-800">{{ $step['judul'] }}</h4>
                                            <p class="text-gray-600">{{ $step['deskripsi'] }}</p>
                                        </div>
                                    @endforeach
                                </div>
                            </div>
                        @endforeach
                    </div>
                @else
                    <div class="p-6 text-gray-900">
                        Konten tidak ditemukan.
                    </div>
                @endif
            </div>
        </div>
    </div>
</x-app-layout>

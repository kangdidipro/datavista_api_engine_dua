<x-app-layout>
    <x-slot name="header">
        <h2 class="font-semibold text-xl text-gray-800 leading-tight">
            {{ __('Teknologi Terkait') }}
        </h2>
    </x-slot>

    <div class="py-12">
        <div class="max-w-7xl mx-auto sm:px-6 lg:px-8">
            <div class="bg-white overflow-hidden shadow-sm sm:rounded-lg">
                @if(!empty($bantuanHelp['bantuan_help'][1]['konten']))
                    @php $content = $bantuanHelp['bantuan_help'][1]['konten']; @endphp
                    <div class="p-6 bg-white border-b border-gray-200">
                        <h3 class="text-lg font-semibold text-gray-900">{{ $content['judul'] }}</h3>
                        <p class="mt-1 text-gray-600">{{ $content['deskripsi'] }}</p>
                        <div class="mt-6 space-y-4">
                            @foreach($content['teknologi'] as $tech)
                                <div class="p-4 bg-gray-50 rounded-lg">
                                    <h4 class="font-bold text-gray-800">{{ $tech['nama'] }}</h4>
                                    <p class="text-gray-600">{{ $tech['deskripsi'] }}</p>
                                </div>
                            @endforeach
                        </div>
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

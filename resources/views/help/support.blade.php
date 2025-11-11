<x-app-layout>
    <x-slot name="header">
        <h2 class="font-semibold text-xl text-gray-800 leading-tight">
            {{ __('Hubungi Dukungan') }}
        </h2>
    </x-slot>

    <div class="py-12">
        <div class="max-w-7xl mx-auto sm:px-6 lg:px-8">
            <div class="bg-white overflow-hidden shadow-sm sm:rounded-lg">
                @if(!empty($bantuanHelp['bantuan_help'][4]['konten']))
                    @php $content = $bantuanHelp['bantuan_help'][4]['konten']; @endphp
                    <div class="p-6 bg-white border-b border-gray-200">
                        <h3 class="text-lg font-semibold text-gray-900">{{ $content['judul'] }}</h3>
                        <p class="mt-1 text-gray-600">{{ $content['deskripsi'] }}</p>
                        <div class="mt-6">
                            <dl>
                                @foreach($content['info_kontak'] as $item)
                                    <div class="bg-gray-50 px-4 py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6 mb-2 rounded-lg">
                                        <dt class="text-sm font-medium text-gray-900">{{ $item['metode'] }}</dt>
                                        <dd class="mt-1 text-sm text-gray-700 sm:mt-0 sm:col-span-2">{{ $item['detail'] }}</dd>
                                    </div>
                                @endforeach
                            </dl>
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

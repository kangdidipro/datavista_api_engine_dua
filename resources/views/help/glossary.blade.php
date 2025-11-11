<x-app-layout>
    <x-slot name="header">
        <h2 class="font-semibold text-xl text-gray-800 leading-tight">
            {{ __('Glosarium') }}
        </h2>
    </x-slot>

    <div class="py-12">
        <div class="max-w-7xl mx-auto sm:px-6 lg:px-8">
            <div class="bg-white overflow-hidden shadow-sm sm:rounded-lg">
                @if(!empty($bantuanHelp['bantuan_help'][0]['konten']))
                    <div class="p-6 bg-white border-b border-gray-200">
                        <dl>
                            @foreach($bantuanHelp['bantuan_help'][0]['konten'] as $item)
                                <div class="bg-gray-50 px-4 py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6 mb-4 rounded-lg">
                                    <dt class="text-sm font-medium text-gray-900">{{ $item['istilah'] }}</dt>
                                    <dd class="mt-1 text-sm text-gray-700 sm:mt-0 sm:col-span-2">{{ $item['definisi'] }}</dd>
                                </div>
                            @endforeach
                        </dl>
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

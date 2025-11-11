<x-app-layout>
    <x-slot name="header">
        <h2 class="font-semibold text-xl text-gray-800 leading-tight">
            {{ __('FAQ (Pertanyaan Umum)') }}
        </h2>
    </x-slot>

    <div class="py-12">
        <div class="max-w-7xl mx-auto sm:px-6 lg:px-8">
            <div class="bg-white overflow-hidden shadow-sm sm:rounded-lg">
                @if(!empty($bantuanHelp['bantuan_help'][3]['konten']))
                    <div class="p-6 bg-white border-b border-gray-200">
                        <div class="space-y-6">
                            @foreach($bantuanHelp['bantuan_help'][3]['konten'] as $faq)
                                <div x-data="{ open: false }">
                                    <button @click="open = !open" class="w-full text-left">
                                        <div class="flex justify-between items-center p-4 bg-gray-50 rounded-lg hover:bg-gray-100">
                                            <h4 class="font-semibold text-gray-800">{{ $faq['pertanyaan'] }}</h4>
                                            <svg class="w-4 h-4 transition-transform duration-200" :class="{'rotate-180': open}" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>
                                        </div>
                                    </button>
                                    <div x-show="open" x-transition class="p-4 text-gray-600">
                                        {{ $faq['jawaban'] }}
                                    </div>
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

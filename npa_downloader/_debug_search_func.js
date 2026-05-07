/*
Подключение стайлера "календарь"
*/
	$(function(){
		$('.js-calendar-call').each(function(){
			var $this=$(this);
				$this.datepicker({
					language: 'ru',
					autoclose: true,
					format: 'dd-mm-yyyy',
					defaultViewDate: 'today',
				}).on('changeDate', function(e) {
					var $this=$(this), 
						$p = $this.parents('.data-inp') || null,
						$inp = $p === null ? null : $p.find('input.form-inp:first');
						if ($inp !== null && $inp.length>0)
							$inp.val(e.format());
					console.log(e);
				});
		}); 
	});
/*
IFPI routines for ajax
*/
// установка параметров и шаблонов URL
	var ifpi_url_tpl = '', // шаблон формирования ссылки поиска ifpi
		ifpi_view_url_tpl='', // шаблон формирования ссылки просмотра ifpi
		ifpi_pager_window=4; //ширина окна пагинации
	function _ifpiSetURITemplate(s) {ifpi_url_tpl = s;}
	function _ifpiSetViewURITemplate(s) {ifpi_view_url_tpl = s;}

	// прокрутка окна к объекту obj
	function obj_scroll_view(obj) {
		setTimeout(function(o){return function(){o.scrollIntoView({block: "start", behavior: "smooth"});};}(obj), 50);
	}
	
	// заставка - крутилка, loader visual
	// b: true - показать, false - скрыть, 
	// без параметров: показать
	function loader_ifpi_ctrl(b) {
		b=b||false;
		if (!b)
			$('#search-tab3').find('.cssload-loader:first').hide();
		else
			$('#search-tab3').find('.cssload-loader:first').fadeIn();
	}

	// формирует html для одной записи, представленной объектом документа d
	// в tpl требуется передать подготовленный шаблон ссылки на просмотр
	// %id% в нем будет заменен идентификатором из d
	function mk_html_ifpi_entry(d,tpl) {
		var buf = '';
		buf += '<p class="h3"><a href="';
		buf += encodeURI(tpl.replace('%id%',d.additionalFields.document_document_link2||''));
		buf += '" class="" target="_blank">';
		buf += d.name+'</a></p>';
//		buf += '<br />';
		buf += '<small>'+d.additionalFields.document_name+'</small>';
		buf += '<p class="descr">';
		buf += '...'+d.snippets.join(' ... ')+'...'+'</p>';
		if (typeof(d.additionalFields.document_date_create) != 'undefined' && d.additionalFields.document_date_create) {
		buf += '<small>Внесен в базу документов: ';
			buf += d.additionalFields.document_date_create.substr(0,d.additionalFields.document_date_create.indexOf('T'));
		if (d.additionalFields.document_action_status)
			buf += ', '+d.additionalFields.document_action_status.toLowerCase();
		buf += '</small>';
		};
		buf += '<hr />';
		return buf;
	}

	// формирует пагинатор, возвращает html код пейджера
	// cp - номер текущей страницы
	// pages - всего страниц
	// fname - имя функции обработчика клика по странице, по умолчанию: call_ifpi_page
	function mk_html_ifpi_pager(cp,pages,fname) {
		fname = fname || 'call_ifpi_page'
		var buf = '<div class="pagination-block pagination-block-search"><div class="modern-page-navigation"><span class="modern-page-title">Страница ';
			buf += cp+' из '+pages+':</span>';
		var pp = cp-1,np=cp+1, pw2=Math.floor(ifpi_pager_window/2),
			ws = cp - pw2,we=cp+pw2,i=0;

		if (np>pages)np=pages; if (pp<1)pp=1;
		if (ws<1) {we+= -(ws+1);ws=1;}
		if (we>pages) {we=pages; 
			if((pages - ifpi_pager_window)>=1)ws=(pages - ifpi_pager_window);}
		buf += '<a class="modern-page-previous" href="javascript:void(0);" onclick="'+fname+'('+pp+');">&lt;&nbsp;Пред.</a>';
		if (ws >= 2) {			
			buf += '<a class="modern-page-first" href="javascript:void(0);" onclick="'+fname+'('+1+');">1</a>';		
			if (ws > 2)
			buf += '<a class="modern-page-dots" href="javascript:void(0);" onclick="'+fname+'('+pp+');">...</a>';
		} 
		for(i=ws;i<=we;i++) {
		if (i!=cp)
			buf += '<a href="javascript:void(0);" class="" onclick="'+fname+'('+i+');">'+i+'</a>';
		else 
			buf += '<span class="modern-page-first modern-page-current">'+cp+'</span>';			
		}
		if (we <( pages-1)) {
			buf += '<a class="modern-page-dots" href="javascript:void(0);" onclick="'+fname+'('+np+');">...</a>';
			buf += '<a href="#search-tab3" class="" onclick="'+fname+'('+pages+');">'+pages+'</a>';
		}
		buf += '<a class="modern-page-next" href="javascript:void(0);" onclick="'+fname+'('+np+');">След.&nbsp;&gt;</a>'
//		buf += '<span class="modern-page-title">Всего: '+pages+'</span>'
		buf += '</div></div>';
		return buf;
	}	
	
	// хранит статус и строку запроса, объект формы
	var _ifpi_query_string ='',_ifpi_load_page=false, _ifpi_form_obj=null;

	// вызов загрузки определенной страницы результата с номером p 
	// осуществляет прокрутку окна и очистку текущего вывода результата
	function call_ifpi_page(p) {		
		_ifpi_load_page=true;
		var $sresult=$('#id-ifpi-search-result');
		obj_scroll_view($sresult.parent().get(0));
		$sresult.html('');
		call_ifpi(_ifpi_query_string, p, _ifpi_form_obj);
	}	

	// формирование запроса к серверу
	// f - (class Document::Form) - объект html формы с параметрами
	// p - номер страницы начала загрузки результата
	// q - строка запроса в кодировке UTF-8
	function call_ifpi(q,p,f) {
		if (ifpi_url_tpl === '') return;		
		_ifpi_query_string =q;
		loader_ifpi_ctrl(true);
		var _src='default',_type='ALL';
		if (typeof(f) != 'undefined' && f !== null && f !== false) {
		   _src=f.src.value;_type=f.stype.value;
		   _ifpi_form_obj=f;
		}		
		// Обращение к прокси-апи php->soap до серверов IFPI по шаблону ifpi_url_tpl
		$.ajax({
			url: ifpi_url_tpl.replace('%p%', p).replace('%q%', q).replace('%s%', _src).replace('%t%',_type),
			dataType: 'JSON',
			method: 'GET',
			context: $('#id-ifpi-search-result'),
			success: function(d,h,c) {				
				if (d && typeof(d.error)!= 'undefined') {
					switch(d.error) {
						case 2:
							this.html('<p>Документов не найдено.</p>');
							break;
						case 0:
							buf ='';
							// подготовка шаблона url просмотра
							tpl = ifpi_view_url_tpl.replace('%query%', d.query||'')
								.replace('%type%',d.stype||'')
								.replace('%bundle%',d.ssrc||'');

							// формирование кода списка результатов
							for(var i=0,ic=d.documents.length;i<ic;i++) {
								buf+=mk_html_ifpi_entry(d.documents[i],tpl);													
							}
							buf += mk_html_ifpi_pager(d.page, d.pages);

							// выгрузка html кода в блок результата
							this.html(buf);
							if (_ifpi_load_page) {
								_ifpi_load_page =false;
								obj_scroll_view(this.parent().get(0));
							}
							break;
						case 1:
						default:
							this.html('<p>Произошла ошибка, попробуйте повторить запрос позднее.</p>');
					}
				} else
					this.html('<p>Произошла ошибка, попробуйте повторить запрос позднее.</p>');
				loader_ifpi_ctrl(false);
			},
			error: function(d,e) {
				loader_ifpi_ctrl(false);
				this.html('<p>Произошла ошибка, попробуйте повторить запрос позднее.</p>');
				console.log(['error',d]);
			}
		});
	}
/*
	Публикации AJAX методы
*/
	// заставка - крутилка, loader visual
	// b: true - показать, false - скрыть, 
	// без параметров: показать
	function loader_pub_ctrl(b,scr) {
		b=b||false;scr=scr||false
		if (!b)
			$('#search-tab2').find('.cssload-loader:first').hide();
		else
			if (scr)
				obj_scroll_view($('#search-tab2').find('.cssload-loader:first').fadeIn().get(0));
			else
				$('#search-tab2').find('.cssload-loader:first').fadeIn();
	}
	// Вывод результата или его очистка
	// text : string - html представление результата
	// ctx : obj$$ - JQuery объект блока результата - необязательный параметр
	function pub_show_result(text, ctx) {
		text = text || ''; ctx = ctx || null ;
		if (ctx === null || ctx.length === 0) {
			ctx = $('#id-pub-search-result');
		}
		ctx.html(text);		
	}
	// Вывод ошибки в результат с очисткой
	// s : string - строка ошибки или '' для очистки результата.
	function pub_show_error(s) {
		s = s || '';
		pub_show_result('<p class="search-error">'+s+'</p>');		
	}
	// Вывод html для записи документа в публикациях
	// d - объект массива документа из ответа
	function mk_html_pub_entry(d) {
		var buf = '';
		buf += '<p class="h3">';
		buf += '<a href="';
		buf += encodeURI(d.link||'');
		buf += '" class="" target="_blank">';
		buf += d.name+'</a>';
		buf += '</p>';
//		buf += '<br />';
		buf += '<small>'+d.text+'</small>';
		buf += '<div class="search-pub-addons">'
		if (typeof(d.EoNumber) != 'undefined') {
			buf += '<small>Номер опубликования: ';
			buf += d.EoNumber;
			buf += '</small>';
		}
		if (typeof(d.pubDate) != 'undefined') {
		buf += '<small>Дата опубликования: ';
			buf += d.pubDate;
			buf += '</small>';
		}
		buf += '</div>';
		buf += '<hr />';
		return buf;
	}
	// вызов загрузки определенной страницы результата с номером p 
	// осуществляет прокрутку окна и очистку текущего вывода результата
	var _pub_form_obj = null;
	function call_publ_page(p) {		
		_ifpi_load_page=true;
		var $sresult=$('#id-pub-search-result');
		obj_scroll_view($sresult.get(0));
		$sresult.html('');
		call_publ(_ifpi_query_string, p, _pub_form_obj);
	}
// вызов проксирования запроса
	// f - (class Document::Form) - объект html формы с параметрами
	// p - номер страницы начала загрузки результата
	// q - строка запроса в кодировке UTF-8
// результат: заполнен блок результата или выведена ошибка
function call_publ(q,p,f) {
	var $f = $(f), $context = $('#id-pub-search-result');
	p = parseInt(p || '1'); if (p<1)p=1;
	if ($f.length > 0 && $context.length>0) {
		_pub_form_obj = $f;
		loader_pub_ctrl(true);
		$.ajax({
			url: '/search/public.php',
			dataType: 'JSON',
			data: $f.serialize()+'&page='+p,
			method: 'POST',
			context: $context,
			success: function(d,h,c) {				
				loader_pub_ctrl(false);
				if (d && typeof(d.error)!= 'undefined') {
					if (d.error>0) {
						pub_show_error(d.msg);
					} else { // выдача для пользователя
						buf = "";
						if (d.documents.length === 0) 
							pub_show_error(d.msg);
						else {
							for (var i=0,ic=d.documents.length;i<ic;i++) {
								buf+=mk_html_pub_entry(d.documents[i]);
							}
							buf += mk_html_ifpi_pager(d.page, d.pages, 'call_publ_page');
							pub_show_result(buf, this);
							obj_scroll_view(this.get(0));
						}
					}
				} else pub_show_error('Произошла ошибка! Пожалуйста обновите страницу и попробуйте еще раз.');
			},
			error: function (d, h) {
				loader_pub_ctrl(false);
				pub_show_error('Произошла ошибка! Пожалуйста обновите страницу и попробуйте еще раз.');
				console.log('PUB: AJAX error');
			}
		});
		pub_show_error('Не заполнены поля запроса: укажите дату, номер, название, часть номера или названия.')
	} else pub_show_error('Произошла ошибка! Пожалуйста обновите страницу и попробуйте еще раз.');
}
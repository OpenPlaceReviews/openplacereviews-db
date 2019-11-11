/* Set the width of the side navigation to 25% */
function openNav() {
    document.getElementById("mySidenav").style.width = "33%";
}

/* Set the width of the side navigation to 0 */
function closeNav() {
    document.getElementById("mySidenav").style.width = "0%";
}

function setCookie(cname, cvalue, exdays) {
    var d = new Date();
    d.setTime(d.getTime() + (exdays*24*60*60*1000));
    var expires = "expires="+ d.toUTCString();
    document.cookie = cname + "=" + cvalue + ";" + expires + ";path=/";
}

function getCookie(cname) {
    var name = cname + "=";
    var decodedCookie = decodeURIComponent(document.cookie);
    var ca = decodedCookie.split(';');
    for(var i = 0; i <ca.length; i++) {
        var c = ca[i];
        while (c.charAt(0) == ' ') {
            c = c.substring(1);
        }
        if (c.indexOf(name) == 0) {
            return c.substring(name.length, c.length);
        }
    }
    return "";
}

function addTripAdvisor() {
    let strUrl = $("#tripAdvisorURL").val();
    let indexG = strUrl.indexOf("-g") + 1;
    let indexD = strUrl.indexOf("-d");
    let keyG = strUrl.substring(indexG, indexD);
    let urlPath = strUrl.substring(indexD + 1);
    let indexEndD = urlPath.indexOf("-");
    let keyD = urlPath.substring(0, indexEndD);
    let placeId = $("#placeId").html();
    let array = [keyG, keyD];
    $.getJSON("/profile/json/", {}, function (data) {
        $("#account_name").html(data.username);
        let obj = {
            "name": data.username + ":openplacereviews",
            "pwd": "",
            "privateKey": data.private_key,
            "addToQueue": true,
            "dontSignByServer": true
        };
        let params = $.param(obj);
        let id = placeId.split(",");
        let editOp = {
            type: "opr.place",
            edit: [{
                id: id,
                change: {
                    "source.tripadvisor": {
                        append: {
                            "id" : array
                        }
                    }
                },
                current: {}
            }]
        };
        $.ajax({
            url: '/api/auth/process-operation?' + params,
            type: 'POST',
            data: JSON.stringify(editOp),
            contentType: 'application/json; charset=utf-8'
        })
            .done(function (data) {
                $("#tripadvisor-exist-div").removeClass("hidden");
                $("#missing-tripadvisor").addClass("hidden");
                $("#trip-advisor-add-block").addClass("hidden");
                console.log(data);
                $("#tripadv-successfull").html("https://dev.openplacereviews.org/api/admin?view=operations&loadBy=id&key="+data.hash).attr("href", "https://dev.openplacereviews.org/api/admin?view=operations&loadBy=id&key="+data.hash);
            })
            .fail(function (xhr, status, error) { alert(error) });
    });

}

function readURL(input) {
    if (input.files && input.files[0]) {
        var reader = new FileReader();

        reader.onload = function (e) {
            $('#img-upload').attr('src', e.target.result);
        };

        reader.readAsDataURL(input.files[0]);
    }
}

function fillTemplateFields(newTemplate, key, value) {
    var newKey = key;
    if (newKey.includes(".")) {
        newKey = newKey.split(".")[1];
    }
    if (newKey.includes(":")) {
        newKey = newKey.split(":")[0];
    }
    newTemplate.find("[did='tag-k']").html(key).attr("href", "https://wiki.openstreetmap.org/wiki/Uk:Key:" + newKey + "?uselang=uk");
    newTemplate.find("[did='tag-v']").html(value);
}

function sidenavReady() {
    $(document).on('change', '.btn-file :file', function() {
        var input = $(this),
            label = input.val().replace(/\\/g, '/').replace(/.*\//, '');
        input.trigger('fileselect', [label]);
    });

    $('.btn-file :file').on('fileselect', function(event, label) {

        var input = $(this).parents('.input-group').find(':text'),
            log = label;

        if( input.length ) {
            input.val(log);
        } else {
            if( log ) alert(log);
        }

    });

    $("#image-file").change(function(){
        readURL(this);
    });

    $("#add-trip-advisor").click(function () {
        addTripAdvisor();
    });

    $('.panel-collapse').on('show.bs.collapse', function () {
        $(this).siblings('.panel-heading').addClass('active');
    });

    $('.panel-collapse').on('hide.bs.collapse', function () {
        $(this).siblings('.panel-heading').removeClass('active');
    });

    $.getJSON("/profile/json/", {}, function (data) {
        if (data.username === "") {
            $("#trip-advisor-add-block").addClass("hidden");
        } else {
            $("#trip-advisor-add-block").removeClass("hidden");
        }
        $("#account_name").html(data.username);
    });
}

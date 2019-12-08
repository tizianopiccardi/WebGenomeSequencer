package main

import (
	"fmt"
	"github.com/PuerkitoBio/purell"
	"net/url"
	"strings"
	"testing"
)

func TestLinkFormat(t *testing.T) {

	contextUrl,_ := url.Parse("http://www.phoenix-adv.com/art/2019/7/27/art_21572_2949197.html")
	//u := "//www.nqwgcn.com.cn/Products/huagongshebei/zhengliuqi、zhengliushuiji/1-277.htm"
	u := "https://www.google.de/#q=54%+Künstler+kultnet"


	u = strings.Replace(u, "\u0008", "", -1)

	hrefUrl, err := url.Parse(u)
	fmt.Println(hrefUrl)
	fmt.Println(err)

	var fragment string
	if err == nil {
		fragment = hrefUrl.Fragment
		hrefUrl = contextUrl.ResolveReference(hrefUrl)
		fmt.Println(purell.NormalizeURL(hrefUrl, PURELL_FLAGS), fragment)
	} else {
		fmt.Errorf(err.Error())
	}
}